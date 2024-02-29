import collections
import datetime
import logging.config
import os
import pathlib
import shutil
import subprocess
from typing import Annotated, TypeAlias

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


def human_readable_size(size: int, decimal_places: int = 2) -> str:
    if size is None:
        return "? B"
    for unit in ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]:
        if size < 1024.0 or unit == "PiB":  # noqa: PLR2004
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit}"


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
    dir_path: pathlib.Path
    mime_type: str | None  # download locally as this type...
    file_name: str
    archive_file_name: str | None


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

    def make_local_file_info(self, dir_path: pathlib.Path, archive: bool):
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
            archive_file_name = f"{file_name}.zip" if archive else None
            obj.local_info = LocalInfo(
                dir_path=dir_path,
                mime_type=dst_mime_type,
                file_name=file_name,
                archive_file_name=archive_file_name,
            )


Tree: TypeAlias = collections.defaultdict[
    str,
    Annotated[TreeNode, pydantic.Field(default_factory=TreeNode)],
]


class Syncer(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)

    # The whole remote directory tree (flattened)
    tree: Tree
    root_folder_id: str
    base_dir: pathlib.Path
    stats: Stats
    archive: bool
    password: str | None

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(Exception),
        wait=tenacity.wait_random_exponential(),
        stop=tenacity.stop_after_attempt(5),
        before_sleep=tenacity.before_sleep_log(logger, logging.INFO),
        reraise=True,
    )
    def download_file(self, obj: RemoteObj, file_path: pathlib.Path):
        logger.info(
            "Downloading\n%s%s.",
            file_path,
            "" if not obj.local_info.mime_type else f"\n(as {obj.local_info.mime_type})",
        )
        obj.gdrive_file.GetContentFile(str(file_path), mimetype=obj.local_info.mime_type)

        self.stats.synced_file_count += 1

        if not self.archive:
            return file_path
        return self.archive_file(file_path)

    def archive_file(self, file_path: pathlib.Path) -> pathlib.Path:
        """Archive the file, optionally with a password."""
        zip_path = file_path.with_name(file_path.name + ".zip")
        command = ["7z", "a"]
        if self.password:
            command.append(f"-p{self.password}")
        command.extend(["-y", zip_path, file_path])

        logger.debug("Archiving")
        output = subprocess.check_output(command)  # noqa: S603
        logger.debug("%s", output)

        file_path.unlink()

        return zip_path

    def check_file_synced(self, obj: RemoteObj):
        remote_mod_time = obj.modified_date.timestamp()
        file_name = obj.local_info.archive_file_name or obj.local_info.file_name
        file_path = obj.local_info.dir_path / file_name

        if not file_path.exists():
            return False

        local_mod_time = file_path.stat().st_mtime
        if local_mod_time == remote_mod_time:
            logger.info("File exists with the same timestamp. Skipping.")
            self.stats.skipped_file_count += 1
            return True

        logger.info("File exists with a different timestamp.")
        return False

    def sync_file(self, obj: RemoteObj):
        """Download a Google Drive file to the specified directory."""
        file_path = obj.local_info.dir_path / obj.local_info.file_name
        logger.info(
            "Syncing file\n %s\n%s, %s %s",
            file_path,
            human_readable_size(obj.file_size),
            obj.mime_type,
            obj.id,
        )

        remote_mod_time = obj.modified_date.timestamp()

        if self.check_file_synced(obj):
            return file_path

        file_path = self.download_file(obj, file_path)

        # Set local file timestamp
        os.utime(file_path, (remote_mod_time, remote_mod_time))

    def delete_removed_files(self, dir_path: pathlib.Path, file_names_seen: set[str]):
        """Walk over the existing files on disk, and delete the ones, which are not on remote."""
        for file_path in dir_path.iterdir():
            if file_path.name in file_names_seen:
                continue
            if file_path.is_dir():
                logger.info("Removing directory\n%s", file_path)
                shutil.rmtree(file_path)
            else:
                logger.info("Removing file\n%s", file_path)
                self.stats.deleted_file_count += 1
                self.stats.deleted_file_size += file_path.stat().st_size
                file_path.unlink()

    def sync_folder(self, folder_id, path: list[str]) -> str:
        """Recursively sync files and directories from the given remote folder to a local folder."""
        node = self.tree[folder_id]
        path.append(node.title)
        logger.info("Syncing folder\n%s", " / ".join(path))

        dir_path = self.base_dir.joinpath(*path)
        dir_path.mkdir(exist_ok=True)

        node.make_local_file_info(dir_path, archive=self.archive)
        file_names_seen: set[str] = set()  # processed files in the current directory
        for obj in node.files:
            self.sync_file(obj)
            file_names_seen.add(obj.local_info.archive_file_name or obj.local_info.file_name)

        for folder_id in node.folders:
            folder_name = self.sync_folder(folder_id, path)
            file_names_seen.add(folder_name)

        # Delete files which have been removed on remote
        self.delete_removed_files(dir_path, file_names_seen)

        path.pop()
        return node.title

    def sync(self):
        self.sync_folder(self.root_folder_id, [])


def get_all_remote_objs(drive: pydrive2.drive.GoogleDrive) -> list[RemoteObj]:
    logger.info("Getting list of all remote files and folders.")
    all_files = drive.ListFile().GetList()
    return [RemoteObj(gdrive_file=file, **file) for file in all_files]


def get_tree(
    drive: pydrive2.drive.GoogleDrive,
    *,
    base_dir: pathlib.Path,
    archive: bool,
    password: str,
) -> Syncer:
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
            stats.total_file_size += obj.file_size or 0

    logger.info(
        "%s files and %s folders found on remote.",
        stats.total_file_count,
        stats.total_folder_count,
    )

    return Syncer(
        stats=stats,
        tree=tree,
        root_folder_id=root_folder_id,
        base_dir=base_dir,
        archive=bool(archive or password),
        password=password,
    )


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


@click.command(context_settings={"show_default": True})
@click.option("--browser", default=None, help="Path to a non-default browser to use for auth")
@click.option("--dir", "base_dir", default="~/Downloads/gdrive", help="Where to save the files")
@click.option("--archive", type=bool, default=True, help="Archive each file")
# @click.option(
#     "--password",
#     prompt=True,
#     default=lambda: os.environ.get("PASSWORD", ""),
#     hide_input=True,
#     help="Password for file archives",
# )
def main(*, browser: str | None, base_dir: str, archive: bool, password: str = "password"):
    if browser:
        os.environ["BROWSER"] = browser

    drive = get_drive_client()
    logger.info("Base dir: %s", base_dir)
    base_dir = pathlib.Path(base_dir).expanduser()
    syncer = get_tree(drive, base_dir=base_dir, archive=archive, password=password)
    syncer.sync()
    logger.info("Stats: %s", syncer.stats)


if __name__ == "__main__":
    main()
