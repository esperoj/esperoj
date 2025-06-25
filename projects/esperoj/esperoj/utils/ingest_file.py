import json
import subprocess
from pathlib import Path

from esperoj.database import get_database
from esperoj.database.models import File
from esperoj.log import get_logger
from esperoj.utils import get_util

logger = get_logger(__name__)
calculate_hash = get_util("calculate_hash")
upload = get_util("upload")
files = get_database("primary").get_table("files")


def ingest_file(file_path: Path, mirrors: list[str]) -> File:
    logger.info("Started to ingest file '%s'", file_path.name)
    metadata = json.loads(
        subprocess.run(["exiftool", "-j", str(file_path)], check=True, capture_output=True, encoding='utf-8').stdout
    )[0]

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
        return files.create({**dict(upload_result), "src": None, "metadata": metadata})
