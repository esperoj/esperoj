from pathlib import Path

from esperoj.database.models import File
from esperoj.logging import get_logger
from esperoj.utils import get_util

logger = get_logger(__name__)
calculate_hash = get_util("calculate_hash")
upload = get_util("upload")


def ingest_file(file_path: Path, mirrors: list[str]) -> File:
    logger.info("Started to ingest file '%s'", file_path.name)
    with file_path.open("rb") as file:
        upload_info = {
            "name": file_path.name,
            "mirrors": {mirror: {"sources": [], "encrypted": False} for mirror in mirrors},
            "sha256": calculate_hash(file, algorithm="sha256"),
            "size": file_path.stat().st_size,
            "src": file_path,
        }
        upload_result = upload([upload_info])[0]
        logger.info("Ingested file '%s'", file_path.name)
        return File(**dict(upload_result), metadata={})
