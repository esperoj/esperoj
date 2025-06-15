from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from os import getenv
from pathlib import Path
from secrets import randbelow
from shutil import move
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Annotated

from py7zr import SevenZipFile
from pydantic import BaseModel, Field

from esperoj.database.models import MirrorInfo
from esperoj.exceptions import VerificationError
from esperoj.log import get_logger
from esperoj.storage import FileHost, Storage, get_mirror
from esperoj.utils import get_util

logger = get_logger(__name__)
calculate_hash = get_util("calculate_hash")


class DownloadInfo(BaseModel):
    name: Annotated[str, Field(min_length=1, max_length=255)]
    mirrors: dict[str, MirrorInfo]
    sha256: Annotated[str, Field(min_length=64, max_length=64)]
    size: Annotated[int, Field(gt=0)]
    dest: Path


def choose_mirror(download_info: DownloadInfo) -> FileHost | Storage:
    mirrors = [
        {
            "name": mirror,
            "probabilities": {
                "small": get_mirror(mirror).probabilities["small"],
                "large": get_mirror(mirror).probabilities["large"],
            },
        }
        for mirror in download_info.mirrors
        if len(download_info.mirrors[mirror]["sources"]) > 0
    ]
    file_category = "small" if download_info.size < 10 * 2**20 else "large"
    total_score = sum(mirror["probabilities"][file_category] for mirror in mirrors)
    chosen_mirror = randbelow(total_score)
    cumulative = 0
    result = get_mirror(mirrors[0]["name"])

    for _, mirror in enumerate(mirrors):
        cumulative += mirror["probabilities"][file_category]
        if chosen_mirror < cumulative:
            result = get_mirror(mirror["name"])
            break

    return result


def download(download_info_list: list) -> Iterable[tuple[Exception | None, DownloadInfo]]:
    default_password = getenv("ENCRYPTION_PASSPHRASE")

    def download_file(download_info: DownloadInfo):
        try:
            mirror = choose_mirror(download_info)
            mirror_info = download_info.mirrors[mirror.name]
            sources = mirror_info["sources"]
            encrypted = mirror_info["encrypted"]
            password = default_password if encrypted else None
            logger.info("Downloading file '%s' from mirror '%s'", download_info.name, mirror.name)

            def download_to_path(path):
                with path.open("wb") as file:
                    for source in sources:
                        for chunk in mirror.stream(source["src"]):
                            file.write(chunk)

            with TemporaryDirectory() as tmpdirname, NamedTemporaryFile(dir=tmpdirname, delete=False) as tmpfilename:
                tmpdir = Path(tmpdirname)
                file_path = tmpdir / download_info.name
                if len(sources) > 1 or encrypted:
                    download_to_path(tmpdir / tmpfilename.name)
                    with SevenZipFile(str(tmpdir / tmpfilename.name), "r", password=password) as archive:
                        archive.extractall(path=str(tmpdir))
                else:
                    download_to_path(file_path)
                with file_path.open("rb") as file:
                    sha256 = calculate_hash(file)
                    if sha256 != download_info.sha256:
                        logger.error(
                            "Hashes mismatch for file '%s':\n'%s'\n'%s'",
                            download_info.name,
                            download_info.sha256,
                            sha256,
                        )
                        raise VerificationError(file_names=[download_info.name])
                move(file_path, download_info.dest)
            return (None, download_info)
        except Exception as e:
            logger.error("Exception :: ", e)
            return (e, download_info)

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(download_file, DownloadInfo(**download_info)): download_info
            for download_info in download_info_list
        }
        return [future.result() for future in as_completed(futures)]
