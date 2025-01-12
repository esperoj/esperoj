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
from esperoj.logging import get_logger
from esperoj.storage import FileHost, Storage, get_file_host_or_storage
from esperoj.utils import get_util

logger = get_logger(__name__)
calculate_hash = get_util("calculate_hash")


class DownloadInfo(BaseModel):
    name: Annotated[str, Field(min_length=1, max_length=255)]
    mirrors: dict[str, MirrorInfo]
    sha256: Annotated[str, Field(min_length=64, max_length=64)]
    size: Annotated[int, Field(gt=0)]
    dest: Path


def choose_host(download_info: DownloadInfo) -> FileHost | Storage:
    hosts = [
        {
            "name": host,
            "probabilities": {
                "small": get_file_host_or_storage(host).probabilities["small"],
                "large": get_file_host_or_storage(host).probabilities["large"],
            },
        }
        for host in download_info.mirrors
        if len(download_info.mirrors[host]["sources"]) > 0
    ]
    file_category = "small" if download_info.size < 10 * 2**20 else "large"
    total_score = sum(host["probabilities"][file_category] for host in hosts)
    chosen_host = randbelow(total_score)
    cumulative = 0
    result = get_file_host_or_storage(hosts[0]["name"])

    for _, host in enumerate(hosts):
        cumulative += host["probabilities"][file_category]
        if chosen_host < cumulative:
            result = get_file_host_or_storage(host["name"])
            break

    return result


def download(download_info_list: list) -> Iterable[tuple[Exception | None, DownloadInfo]]:
    default_password = getenv("ENCRYPTION_PASSPHRASE")

    def download_file(download_info: DownloadInfo):
        try:
            host = choose_host(download_info)
            mirror = download_info.mirrors[host.name]
            sources = mirror["sources"]
            encrypted = mirror["encrypted"]
            password = default_password if encrypted else None
            logger.info("Downloading file '%s' from host '%s'", download_info.name, host.name)

            with TemporaryDirectory() as tmpdirname:
                tmpdir = Path(tmpdirname)
                with NamedTemporaryFile(dir=tmpdirname, delete=False) as tmpfilename:
                    raw_path = Path(tmpfilename.name)
                    with raw_path.open("wb") as file:
                        for source in sources:
                            for chunk in host.stream(source["src"]):
                                file.write(chunk)

                    file_path = raw_path

                    if len(sources) > 1 or encrypted:
                        with SevenZipFile(str(raw_path), "r", password=password) as archive:
                            archive.extractall(path=str(tmpdir))
                        file_path = tmpdir / download_info.name

                    with file_path.open("rb") as file:
                        if calculate_hash(file) != download_info.sha256:
                            raise VerificationError(file_names=[download_info.name])

                    move(file_path, download_info.dest)
            return (None, download_info)
        except Exception as e:
            logger.error("Exception :: ", e)
            return (e, download_info)

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(download_file, DownloadInfo(**download_info)): download_info
            for download_info in download_info_list
        }
        return [future.result() for future in as_completed(futures)]
