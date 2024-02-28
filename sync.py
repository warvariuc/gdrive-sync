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


class LocalInfo(pydantic.BaseModel):
    dst_mime_type: str | None  # download locally as this type...
    file_name: str


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
    local_info: LocalInfo | None = None

    def human_readable_size(self, decimal_places=2) -> str:
        size = self.file_size
        if size is None:
            return "? B"
        for unit in ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]:
            if size < 1024.0 or unit == "PiB":  # noqa: PLR2004
                break
            size /= 1024.0
        return f"{size:.{decimal_places}f} {unit}"

    @property
    def dst_info(self):
        dst_mime_type, dst_ext = MIMETYPES.get(self.mime_type, (None, None))
        return dst_mime_type, dst_ext


class TreeNode(pydantic.BaseModel):
    title: str = ""
    folders: list[str] = []  # IDs
    files: list[RemoteObj] = []

    def _sanitize_file_name(self, file_name: str, dst_ext: str, file_names_seen: set[str]) -> str:
        """Convert unacceptable filesystem chars to hyphens and add postfixes to duplicate file
        names to make them unique."""
        file_name = file_name.replace("/", "_")
        dst_ext = f".{dst_ext}" if dst_ext else ""
        while True:
            _file_name = file_name + dst_ext
            if _file_name not in file_names_seen:
                return _file_name
            file_name += " (1)"

    def make_local_file_info(self):
        """Make local filesystem info for files in a folder"""
        # There can be several files with the same name in a directory
        # Sort by name, then by date, then add postfix `(1)` to an existing file name
        # Google Drive for Desktop also does this, from what I see
        self.files.sort(key=lambda obj: (obj.title, obj.modified_date, obj.id))

        file_names_seen: set[str] = set()
        for obj in self.files:
            dst_mime_type, dst_ext = MIMETYPES.get(obj.mime_type, (None, None))
            file_name = self._sanitize_file_name(obj.title, dst_ext, file_names_seen)
            file_names_seen.add(file_name)
            obj.local_info = LocalInfo(dst_mime_type=dst_mime_type, file_name=file_name)


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


def process_file(obj: RemoteObj, dir_path: pathlib.Path):
    """Download a Google Drive file to the specified directory.

    Args:
        obj: GDrive file structure
        dir_path: the current remote directory path
    """
    file_path = dir_path / obj.local_info.file_name
    logger.info(
        "Syncing file\n %s\n%s, %s %s",
        file_path,
        obj.human_readable_size(),
        obj.mime_type,
        obj.id,
    )

    remote_mod_time = obj.modified_date.timestamp()

    if file_path.exists():
        local_mod_time = file_path.stat().st_mtime
        if local_mod_time == remote_mod_time:
            logger.info("File exists with the same timestamp. Skipping.")
            return file_path
        logger.info("File exists with a different timestamp.")

    logger.info(
        "Downloading\n%s%s.",
        file_path,
        "" if not obj.local_info.dst_mime_type else f"\n(as {obj.local_info.dst_mime_type})",
    )
    download_file(obj, file_path, obj.local_info.dst_mime_type)

    # Set local file timestamp
    os.utime(file_path, (remote_mod_time, remote_mod_time))


def delete_removed_files(dir_path: pathlib.Path, file_names_seen: set[str]):
    """Walk over the existing files on disk, and delete the ones, which are not on remote."""
    for file_path in dir_path.iterdir():
        if file_path.name in file_names_seen:
            continue
        if file_path.is_dir():
            logger.info("Removing directory\n%s", file_path)
            shutil.rmtree(file_path)
        else:
            logger.info("Removing file\n%s", file_path)
            file_path.unlink()


def process_folder(tree: dict[str, TreeNode], folder_id, path: list[str]) -> str:
    """Recursively sync files and directories from the given remote folder to a local folder.

    Args:
        tree: The whole remote directory tree (flattened)
        folder_id:
        path:
    """
    node = tree[folder_id]
    path.append(node.title)
    logger.info("Syncing folder\n%s", " / ".join(path))

    # TODO: make dir configurable
    dir_path = pathlib.Path("gdrive").joinpath(*path)
    dir_path.mkdir(exist_ok=True)

    node.make_local_file_info()
    file_names_seen: set[str] = set()  # processed files in the current directory
    for obj in node.files:
        process_file(obj, dir_path)
        file_names_seen.add(obj.local_info.file_name)

    for folder_id in node.folders:
        folder_name = process_folder(tree, folder_id, path)
        file_names_seen.add(folder_name)

    delete_removed_files(dir_path, file_names_seen)

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
