import collections
import datetime
import logging.config
import os
import pathlib
import shutil
from typing import TypeAlias

import click
import pydantic
import pydrive2.auth
import pydrive2.drive
import pydrive2.files
import tenacity


logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "standard": {
                "format": "%(asctime)s [%(levelname)s] %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "stdout": {
                "level": "INFO",
                "formatter": "standard",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            "": {  # root logger
                "handlers": ["stdout"],
                "level": "INFO",
            },
        },
    },
)
logger = logging.getLogger()

FOLDER_MIME = "application/vnd.google-apps.folder"
MIMETYPES = {
    "application/vnd.google-apps.spreadsheet": (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "xlsx",
    ),
    "application/vnd.google-apps.document": (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "docx",
    ),
    "application/vnd.google-apps.presentation": (
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "pptx",
    ),
    "application/vnd.google-apps.drawing": ("image/svg+xml", "svg"),
}


class Stats(pydantic.BaseModel):
    total_file_count: int = 0
    total_folder_count: int = 0
    total_file_size: int = 0
    synced_file_count: int = 0
    synced_file_size: int = 0
    skipped_file_count: int = 0
    deleted_file_count: int = 0
    deleted_file_size: int = 0


class RemoteParentFolder(pydantic.BaseModel):
    id: str  # noqa: A003
    is_root: bool = pydantic.Field(validation_alias="isRoot")


class RemoteObj(pydantic.BaseModel):
    """Remote file or folder. A wrapper around GoogleDriveFile."""

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)

    id: str  # noqa: A003
    title: str
    mime_type: str = pydantic.Field(validation_alias="mimeType")
    file_size: int | None = pydantic.Field(default=None, validation_alias="fileSize")
    modified_date: datetime.datetime = pydantic.Field(validation_alias="modifiedDate")
    parents: list[RemoteParentFolder]
    gdrive_file: pydrive2.files.GoogleDriveFile

    def human_readable_size(self, decimal_places=2):
        size = self.file_size
        for unit in ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]:
            if size < 1024.0 or unit == "PiB":  # noqa: PLR2004
                break
            size /= 1024.0
        return f"{size:.{decimal_places}f} {unit}"


class TreeNodeFiles(pydantic.RootModel):
    root: list[RemoteObj]


class TreeNode(pydantic.BaseModel):
    title: str = ""
    folders: list[str] = []  # IDs
    files: TreeNodeFiles = []


Tree: TypeAlias = collections.defaultdict[str, TreeNode]


def get_all_remote_objs(drive: pydrive2.drive.GoogleDrive) -> list[RemoteObj]:
    logger.info("Getting list of all remote files and folders.")
    all_files = drive.ListFile().GetList()
    return [RemoteObj(gdrive_file=file, **file) for file in all_files]


def get_tree(drive: pydrive2.drive.GoogleDrive) -> tuple[Stats, Tree, str | None]:
    all_objs = get_all_remote_objs(drive)

    stats = Stats()
    # the keys are folder ids
    tree: Tree = collections.defaultdict(TreeNode)
    root_folder_id = None
    for obj in all_objs:
        if len(obj.parents) == 0:
            continue
        assert len(obj.parents) == 1
        parent = obj.parents[0]
        if parent.is_root:
            root_folder_id = parent.id
        if obj.mime_type == FOLDER_MIME:
            tree[parent.id].folders.append(obj.id)
            tree[obj.id].title = obj.title
            stats.total_folder_count += 1
        else:
            tree[parent.id].files.append(obj)
            stats.total_file_count += 1

    logger.info(
        "%s files and %s folders found on remote.",
        stats.total_file_count,
        stats.total_folder_count,
    )

    return stats, tree, root_folder_id


@tenacity.retry(
    retry=tenacity.retry_if_exception_type(Exception),
    wait=tenacity.wait_random_exponential(),
    stop=tenacity.stop_after_attempt(5),
    before_sleep=tenacity.before_sleep_log(logger, logging.INFO),
    reraise=True,
)
def download_file(obj: RemoteObj, file_path: pathlib.Path, mime_type: str):
    # TODO: save the file encrypted
    obj.gdrive_file.GetContentFile(str(file_path), mimetype=mime_type)


def process_file(obj: RemoteObj, dir_path: pathlib.Path, file_names: set[str]) -> str:
    """Download a Google Drive file to the specified directory.

    Args:
        obj: GDrive file structure
        dir_path: the current remote directory path
        file_names: names of fiels already synced in this directory

    Returns:
        str: path to the downloaded file
    """
    logger.info(
        "Syncing file: %s (%s, %s %s)",
        obj.title,
        obj.human_readable_size(),
        obj.mime_type,
        obj.id,
    )

    dst_mime_type, dst_ext = MIMETYPES.get(obj.mime_type, (None, None))

    # TODO: do not pass file_names and dir_path here
    # add "_local" key to `file` with {"file_path": ..., "mime_type": ...}
    file_name = sanitize_file_name(obj.title, dst_ext, file_names)
    file_path = dir_path / file_name

    remote_mod_time = obj.modified_date.timestamp()

    if file_path.exists():
        local_mod_time = file_path.stat().st_mtime
        if local_mod_time == remote_mod_time:
            logger.debug("File exists with the same timestamp. Skipping.")
            return file_name

    logger.info(
        "Downloading %s%s.",
        file_path,
        "" if not dst_mime_type else f" (as {dst_mime_type})",
    )
    download_file(obj, file_path, dst_mime_type)

    # Set local file timestamp
    os.utime(file_path, (remote_mod_time, remote_mod_time))

    return file_name


def delete_removed_files(dir_path: pathlib.Path, all_file_names: set[str]):
    """Walk over the existing files on disk, and delete the ones, which are not on remote."""
    for file_name in os.listdir(dir_path):
        if file_name in all_file_names:
            continue
        file_path = dir_path / file_name
        if file_path.is_dir():
            logger.info("Removing directory %s", file_path)
            shutil.rmtree(file_path)
        else:
            logger.info("Removing file %s", file_path)
            file_path.unlink()


def sanitize_file_name(file_name: str, dst_ext: str, all_file_names: set[str]):
    file_name = file_name.replace("/", "_")
    dst_ext = f".{dst_ext}" if dst_ext else ""
    while (file_name + dst_ext) in all_file_names:
        file_name += " (1)"
    file_name += dst_ext
    return file_name


def process_folder(tree: dict[str, TreeNode], folder_id, path: list[str]):
    """Recursively sync files and directories from the given remote folder to a local folder.

    Args:
        tree: The whole remote directory tree (flattened)
        folder_id:
        path:
    """
    node = tree[folder_id]
    path.append(node.title)
    logger.info("Syncing folder %s", " / ".join(path))

    dir_path = pathlib.Path("gdrive").joinpath(*path)
    dir_path.mkdir(exist_ok=True)

    # There can be several files with the same name in a directory
    # Sort by name, then by date, then add postfix `(1)` to an existing file name
    # Google Drive for Desktop also does this, from what I see
    node.files.sort(key=lambda obj: (obj.title, obj.modified_date, obj.id))
    file_names = set()  # processed files in the current directory
    for file in node.files:
        file_name = process_file(file, dir_path, file_names)
        file_names.add(file_name)

    for folder_id in node.folders:
        folder_name = process_folder(tree, folder_id, path)
        file_names.add(folder_name)

    delete_removed_files(dir_path, file_names)

    path.pop()
    return node.title


def get_drive_client() -> pydrive2.drive.GoogleDrive:
    gauth = pydrive2.auth.GoogleAuth(settings_file="settings.yaml")

    gauth.GetFlow()
    gauth.flow.params.update({"access_type": "offline"})
    gauth.flow.params.update({"approval_prompt": "force"})

    # Create local webserver and handle authentication
    try:
        gauth.LocalWebserverAuth()
    except pydrive2.auth.RefreshError as exc:
        if "Access token refresh failed" in str(exc):
            raise Exception("Try to delete credentials.json") from exc
        raise

    try:
        return pydrive2.drive.GoogleDrive(gauth)
    except pydrive2.auth.AuthError as exc:
        raise Exception("Could not create Google Drive client") from exc


@click.command()
@click.option("--browser", help="Path to a non-default browser to use for auth")
def main(browser: str | None = None):
    if browser:
        os.environ["BROWSER"] = browser

    drive = get_drive_client()
    stats, tree, root_folder_id = get_tree(drive)
    process_folder(tree, root_folder_id, [])
    logger.info("Stats: %s", stats)


if __name__ == "__main__":
    main()
