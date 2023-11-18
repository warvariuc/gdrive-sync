import os

import collections
import dataclasses
import datetime
import logging.config
import os.path
import shutil

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
    }
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


@dataclasses.dataclass
class Stats:
    total_file_count: int = 0
    total_folder_count: int = 0
    total_file_size: int = 0
    synced_file_count: int = 0
    synced_file_size: int = 0
    skipped_file_count: int = 0
    deleted_file_count: int = 0
    deleted_file_size: int = 0


def get_all_files(drive: pydrive2.drive.GoogleDrive):
    all_files = drive.ListFile().GetList()
    return all_files


def get_tree(drive: pydrive2.drive.GoogleDrive) -> tuple[Stats, dict, str | None]:
    logger.info("Getting list of all remote files and folders.")
    all_files = get_all_files(drive)

    stats = Stats()
    # the keys are folder ids
    tree = collections.defaultdict(lambda: {"title": "", "folders": [], "files": []})
    root_folder_id = None
    for file in all_files:
        if len(file["parents"]) == 0:
            continue
        assert len(file["parents"]) == 1
        parent = file["parents"][0]
        if parent["isRoot"]:
            root_folder_id = parent["id"]
        if file["mimeType"] == FOLDER_MIME:
            tree[parent["id"]]["folders"].append(file["id"])
            tree[file["id"]]["title"] = file["title"]
            stats.total_folder_count += 1
        else:
            tree[parent["id"]]["files"].append(file)
            stats.total_file_count += 1

    logger.info(
        "%s files and %s folders found on remote.",
        stats.total_file_count,
        stats.total_folder_count,
    )

    return stats, tree, root_folder_id


def human_readable_size(size: int, decimal_places=2):
    for unit in ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]:
        if size < 1024.0 or unit == "PiB":
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit}"


@tenacity.retry(
    retry=tenacity.retry_if_exception_type(Exception),
    wait=tenacity.wait_random_exponential(),
    stop=tenacity.stop_after_attempt(5),
    before_sleep=tenacity.before_sleep_log(logger, logging.INFO),
    reraise=True,
)
def download_file(file: pydrive2.files.GoogleDriveFile, file_path: str, mime_type: str):
    # TODO: save the file encrypted
    file.GetContentFile(file_path, mimetype=mime_type)


def process_file(
    file: pydrive2.files.GoogleDriveFile, dir_path: str, file_names: set[str]
) -> str:
    """Download a Google Drive file to the specified directory.

    Args:
        file: GDrive file structure
        dir_path: the current remote directory path
        file_names: names of fiels already synced in this directory

    Returns:
        str: path to the downloaded file
    """
    logger.info(
        "Syncing file: %s (%s, %s %s)",
        file["title"],
        human_readable_size(int(file.get("fileSize", 0))),
        file["mimeType"],
        file["id"],
    )

    dst_mime_type, dst_ext = MIMETYPES.get(file["mimeType"], (None, None))
    file_name = file["title"]

    # TODO: do not pass file_names and dir_path here
    # add "_local" key to `file` with {"file_path": ..., "mime_type": ...}
    file_name = sanitize_file_name(file_name, dst_ext, file_names)
    file_path = dir_path + "/" + file_name

    remote_mod_time = datetime.datetime.strptime(
        file["modifiedDate"], "%Y-%m-%dT%H:%M:%S.%fZ"
    ).timestamp()

    if os.path.exists(file_path):
        local_mod_time = os.path.getmtime(file_path)
        if local_mod_time == remote_mod_time:
            logger.debug("File exists with the same timestamp. Skipping.")
            return file_name

    logger.info(
        "Downloading %s%s.",
        file_path,
        "" if not dst_mime_type else f" (as {dst_mime_type})",
    )
    download_file(file, file_path, dst_mime_type)

    # Set local file timestamp
    os.utime(file_path, (remote_mod_time, remote_mod_time))

    return file_name


def delete_removed_files(dir_path: str, all_file_names: set[str]):
    """Walk over the existing files on disk, and delete the ones, which are not on remote."""
    for file_name in os.listdir(dir_path):
        if file_name in all_file_names:
            continue
        file_path = os.path.join(dir_path, file_name)
        if os.path.isdir(file_path):
            logger.info("Removing directory %s", file_path)
            shutil.rmtree(file_path)
        else:
            logger.info("Removing file %s", file_path)
            os.remove(file_path)


def sanitize_file_name(file_name: str, dst_ext: str, all_file_names: set[str]):
    file_name = file_name.replace("/", "_")
    dst_ext = f".{dst_ext}" if dst_ext else ""
    while (file_name + dst_ext) in all_file_names:
        file_name += " (1)"
    file_name += dst_ext
    return file_name


def process_folder(tree: dict, folder_id, path: list[str]):
    """Recursively sync files and directories from the given remote folder.

    Args:
        tree: The whole remote directory tree (flattened)
        folder_id:
        path:
    """
    node = tree[folder_id]
    path.append(node["title"])
    logger.info("Syncing folder %s", " / ".join(path))

    dir_path = "gdrive" + "/".join(path)
    os.makedirs(dir_path, exist_ok=True)

    # There can be several files with the same name in a directory
    # Sort by name, then by date, then add postfix `(1)` to an existing file name
    # Google Drive for Desktop also does this, from what I see
    node["files"].sort(
        key=lambda file: (file["title"], file["modifiedDate"], file["id"])
    )
    file_names = set()  # processed files in the current directory
    for file in node["files"]:
        file_name = process_file(file, dir_path, file_names)
        file_names.add(file_name)

    for folder_id in node["folders"]:
        folder_name = process_folder(tree, folder_id, path)
        file_names.add(folder_name)

    delete_removed_files(dir_path, file_names)

    path.pop()
    return node["title"]


def get_drive_client() -> pydrive2.drive.GoogleDrive:
    gauth = pydrive2.auth.GoogleAuth(settings_file='settings.yaml')

    gauth.GetFlow()
    gauth.flow.params.update({"access_type": "offline"})
    gauth.flow.params.update({"approval_prompt": "force"})

    # Create local webserver and handle authentication
    try:
        gauth.LocalWebserverAuth()
    except pydrive2.auth.RefreshError as exc:
        if "Access token refresh failed" in str(exc):
            raise Exception("Try to delete credentials.json") from exc
        else:
            raise

    try:
        return pydrive2.drive.GoogleDrive(gauth)
    except pydrive2.auth.AuthError as exc:
        raise Exception("Could not create Google Drive client") from exc


def main():
    drive = get_drive_client()
    stats, tree, root_folder_id = get_tree(drive)
    process_folder(tree, root_folder_id, [])
    print(f"Stats: {stats}")


if __name__ == "__main__":
    main()
