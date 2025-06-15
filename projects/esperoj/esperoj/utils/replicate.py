from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from tempfile import TemporaryDirectory

from esperoj.database import Record, get_database
from esperoj.database.models import File
from esperoj.log import get_logger
from esperoj.utils import get_util

logger = get_logger(__name__)
upload = get_util("upload")
download = get_util("download")


def is_needed_to_process(file: File) -> bool:
    return any(len(mirror_info["sources"]) == 0 for mirror_info in file.mirrors.values())


def replicate(limit: int = 16) -> list[Record]:
    db = get_database("primary")
    files_table = db.get_table("files")
    files = files_table.query()
    files_to_process = list(filter(is_needed_to_process, files))[:limit]

    def replicate_file(file) -> File:
        try:
            with TemporaryDirectory() as tmpdirname:
                dest = Path(tmpdirname) / file.name
                download_info = {**dict(file), "dest": dest}
                error = download([download_info])[0][0]
                if error:
                    raise error
                mirrors_to_upload = {
                    mirror: mirror_info
                    for mirror, mirror_info in file.mirrors.items()
                    if len(mirror_info["sources"]) == 0
                }
                upload_info = {**dict(file), "src": dest}
                upload_info["mirrors"] = mirrors_to_upload
                upload_result = upload([upload_info])[0]
                file.mirrors = {**file.mirrors, **upload_result.mirrors}
        except Exception as e:
            logger.error("An error occured when replicating file '%s'.", file.name)
            logger.error("Exception :: ", e)
        return file

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(replicate_file, file): file for file in files_to_process}
        new_files = [future.result() for future in as_completed(futures)]
        if len(new_files) == 0:
            logger.info("No file needs to be replicated.")
            return []
        return files_table.batch_update([{"id": file.id, "mirrors": file.mirrors} for file in new_files])
