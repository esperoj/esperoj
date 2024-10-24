import asyncio
import os
import secrets
import tempfile
from collections.ABC import Iterator
from pathlib import Path
from typing import Annotated, Any

from py7zr import SevenZipFile
from pydantic import BaseModel, Field

from esperoj.database.models import MirrorInfo
from esperoj.exceptions import VerificationError
from esperoj.storage.file_host import FileHost
from esperoj.storage.storage import Storage
from esperoj.utils.utils import Utils


class Transfer(BaseModel):
    name: Annotated[str, Field(min_length=1, max_length=255)]
    mirrors: dict[str, MirrorInfo]
    sha256: Annotated[str, Field(min_length=64, max_length=64)]
    size: Annotated[int, Field(gt=0)]
    src: str


class TransferManager:
    def __init__(self, file_hosts: dict[str, FileHost] | None, storages: dict[str, Storage] | None):
        if file_hosts is None and storages is None:
            raise ValueError("At least one of 'file_hosts' or 'storages' must be provided.")
        self.file_hosts = file_hosts
        self.storages = storages
        self.utils = Utils()
        self.password = os.getenv("ENCRYPTION_PASSPHRASE")

    def _choose_host(self, transfer: Transfer) -> FileHost:
        hosts = [
            {
                "name": host,
                "probabilities": {
                    "small": self.file_hosts[host].probabilities["small"],
                    "large": self.file_hosts[host].probabilities["large"],
                },
            }
            for host in transfer.mirrors
        ]
        file_category = "small" if transfer.size < 10 * 2**20 else "large"
        total_score = sum(host["probabilities"][file_category] for host in hosts)
        chosen_host = secrets.randbelow(total_score)
        cumulative = 0
        result = self.file_hosts[hosts[-1]["name"]]
        for _, host in enumerate(hosts):
            cumulative += host["probabilities"][file_category]
            if chosen_host < cumulative:
                result = self.file_hosts[host["name"]]
        return result

    async def download(self, transfer_list: list) -> Iterator[tuple[Exception | None, Transfer]]:
        semaphore = asyncio.Semaphore(8)

        async def download_file(transfer):
            async with semaphore:
                try:
                    host = await self._choose_host(transfer)
                    mirror = transfer.mirrors[host.name]
                    sources = mirror["sources"]
                    is_encrypted = mirror["encrypted"]
                    password = self.password if is_encrypted else None

                    with tempfile.TemporaryDirectory() as tmpdirname:
                        tmpdir = Path(tmpdirname)
                        raw_path = tmpdir / "file.bin"

                        with raw_path.open("wb") as file:
                            for source in sources:
                                for chunk in host.stream(source["src"]):
                                    file.write(chunk)

                        file_path = raw_path

                        if len(sources) > 1 or is_encrypted:
                            with SevenZipFile(str(raw_path), "r", password=password) as archive:
                                archive.extractall(path=tmpdirname)
                            file_path = tmpdir / transfer.name

                        with file_path.open("rb") as file:
                            if self.utils.calculate_hash(file) != transfer.sha256:
                                raise VerificationError(file_names=[transfer.name])

                        file_path.replace(Path(transfer.src))
                    return (None, transfer)
                except Exception as e:
                    return (e, transfer)

        tasks = [download_file(transfer) for transfer in transfer_list]

        for future in asyncio.as_completed(tasks):
            yield await future

    async def upload(self, upload_info_list: list[Transfer]) -> Iterator[tuple[Exception | None, Transfer]]:
        semaphore = asyncio.Semaphore(4)

        async def upload_file(transfer) -> tuple[Exception, Transfer]:
            async with semaphore:
                try:
                    host = await self._choose_host(transfer)
                    for host in [host for hosttransfer.mirrors]
                    mirror = transfer.mirrors[host.name]
                    sources = mirror["sources"]
                    is_encrypted = mirror["encrypted"]
                    password = self.password if is_encrypted else None

                    with tempfile.TemporaryDirectory() as tmpdirname:
                        tmpdir = Path(tmpdirname)
                        raw_path = tmpdir / "file.bin"

                        with raw_path.open("wb") as file:
                            for source in sources:
                                for chunk in host.stream(source["src"]):
                                    file.write(chunk)

                        file_path = raw_path

                        if len(sources) > 1 or is_encrypted:
                            with SevenZipFile(str(raw_path), "r", password=password) as archive:
                                archive.extractall(path=tmpdirname)
                            file_path = tmpdir / transfer.name

                        with file_path.open("rb") as file:
                            if self.utils.calculate_hash(file) != transfer.sha256:
                                raise VerificationError(file_names=[transfer.name])

                        file_path.replace(Path(transfer.src))
                    return (None, transfer)
                except Exception as e:
                    return (e, transfer)

        tasks = [upload_file(transfer) for transfer in transfer_list]

        for future in asyncio.as_completed(tasks):
            yield await future
