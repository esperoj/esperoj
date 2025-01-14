from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from os import getenv
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Any

import multivolumefile
from py7zr import SevenZipFile
from pydantic import BaseModel, Field

from esperoj.database.models import MirrorInfo, SourceInfo
from esperoj.logging import get_logger
from esperoj.storage import FileHost, Storage, get_mirror
from esperoj.utils import get_util

logger = get_logger(__name__)
calculate_hash = get_util("calculate_hash")


class UploadInfo(BaseModel):
    name: Annotated[str, Field(min_length=1, max_length=255)]
    mirrors: dict[str, MirrorInfo]
    sha256: Annotated[str, Field(min_length=64, max_length=64)]
    size: Annotated[int, Field(gt=0)]
    src: Path


def upload(upload_info_list: list[dict[str, Any]]) -> Iterable[UploadInfo]:
    default_password = getenv("ENCRYPTION_PASSPHRASE")

    def upload_file_to_mirror(upload_info: UploadInfo, mirror_name: str) -> MirrorInfo:
        mirror_info = upload_info.mirrors[mirror_name]
        sources = []
        name = upload_info.name
        mirror = get_mirror(mirror_name)
        encrypted = mirror_info["encrypted"]
        password = default_password if encrypted else None
        block_size = getattr(mirror, "max_file_size", 2 * 2**30)
        src = upload_info.src

        logger.info("Uploading file '%s' to mirror '%s'", name, mirror_name)

        try:
            with TemporaryDirectory() as tmpdirname:
                tmpdir = Path(tmpdirname)
                archive_path = f"{tmpdir / name}.7z"
                if upload_info.size > block_size or encrypted:
                    with (
                        multivolumefile.open(archive_path, mode="wb", volume=block_size) as target_archive,
                        SevenZipFile(target_archive, "w", password=password) as archive,  # type: ignore
                    ):
                        archive.write(str(src))
                    files = sorted(file for file in tmpdir.glob("*.7z*"))
                    for file in files:
                        f = file.open("rb")
                        sha256 = calculate_hash(f, algorithm="sha256")
                        f.close()
                        if isinstance(mirror, FileHost):
                            source: SourceInfo = {
                                "src": mirror.upload(str(file)),
                                "sha256": sha256,
                                "size": file.stat().st_size,
                                "verified": False,
                            }
                            sources.append(source)
                        if isinstance(mirror, Storage):
                            mirror.upload(str(src), name)
                            source: SourceInfo = {
                                "src": name,
                                "sha256": sha256,
                                "size": file.stat().st_size,
                                "verified": False,
                            }
                            sources.append(source)
                else:
                    if isinstance(mirror, FileHost):
                        source: SourceInfo = {
                            "src": mirror.upload(str(src)),
                            "sha256": upload_info.sha256,
                            "size": upload_info.size,
                            "verified": False,
                        }
                        sources.append(source)

                    if isinstance(mirror, Storage):
                        mirror.upload(str(src), name)
                        source: SourceInfo = {
                            "src": name,
                            "sha256": upload_info.sha256,
                            "size": upload_info.size,
                            "verified": False,
                        }
                        sources.append(source)

        except Exception as e:
            logger.error("An error occured when uploaded file '%s' to mirror '%s'", name, mirror_name)
            logger.error("Exception :: ", e)
        return {"sources": sources, "encrypted": encrypted}

    def upload_file(upload_info: UploadInfo) -> UploadInfo:
        mirrors = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(upload_file_to_mirror, upload_info, mirror_name): mirror_name
                for mirror_name in upload_info.mirrors
            }
            for future in as_completed(futures):
                mirrors[futures[future]] = future.result()
        return upload_info.model_copy(update={"mirrors": mirrors}, deep=True)

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(upload_file, UploadInfo(**upload_info)): upload_info for upload_info in upload_info_list
        }
        return [future.result() for future in as_completed(futures)]
