"""Ingest util."""

import json
import subprocess
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from esperoj.database.models import File


def ingest(
    esperoj,
    path: Path,
    storage_names: list[str],
    post_process: Callable[[Path, dict, File], File],
    file_hosts: list[str],
) -> list[File]:
    """Ingest a file into the Esperoj system.

    Args:
        esperoj (object): The Esperoj object representing the system.
        path (Path): The path to be ingested.
        storage_names (list[str]): The list of storages to upload.
        post_process (Callable[[Path, dict, File], File]): The function to perform post-processing on the ingested file.
        file_hosts (list[str]): List of file hosts.

    Returns:
        list[File]: The database records representing the ingested files.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        FileExistsError: If the file already exists in the system.
        RuntimeError: If the file type is not supported.
    """
    logger = esperoj.loggers["primary"]
    file_paths = []

    if path.is_dir():
        file_paths = [file_path for file_path in path.iterdir() if file_path.is_file()]
    else:
        if not path.is_file():
            raise FileNotFoundError(f"The specified path {path} does not exist.")
        file_paths = [path]

    def ingest_file(file_path: Path) -> File:
        logger.info(f"Start to ingest `{file_path}`")

        name = file_path.name
        size = file_path.stat().st_size
        f = file_path.open("rb")
        sha256sum = esperoj.utils.calculate_hash(f, algorithm="sha256")
        f.close()
        result = subprocess.run(["exiftool", "-j", str(file_path)], check=True, capture_output=True, text=True)
        metadata = json.loads(result.stdout)[0]
        files = esperoj.databases["primary"].get_table("files")

        def upload() -> File:
            """Upload the file to the storages, and file hosts, then return a database record for it.

            Returns:
                File: The database record representing the ingested file.

            Raises:
                FileExistsError: If the file already exists in any of the storages or database.
            """
            if list(filter(lambda file: file["name"] == name, files.query())) != []:
                raise FileExistsError
            file = {
                "name": name,
                "size": size,
                "sha256": sha256sum,
                "verified": False,
                "metadata": metadata,
                "mirrors": {},
            }
            for storage_name in storage_names:
                try:
                    storage = esperoj.storages[storage_name]
                    storage.upload(str(file_path), name)
                    file["mirrors"][storage_name] = {"sources": [storage_name], "encrypted": False}
                except Exception:
                    logger.error(f"Error when upload file `{name}` from `{storage_name}`")
            for file_host in file_hosts:
                try:
                    url = esperoj.file_hosts[file_host].upload(str(file_path))
                    file["mirrors"][file_host] = {"sources": [url], "encrypted": False}
                except Exception:
                    logger.error(f"Error when upload file `{name}` from `{file_host}`")
            return files.create(file)

        return upload()

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = []
        futures = {executor.submit(ingest_file, file_path): file_path for file_path in file_paths}
        for future in as_completed(futures):
            try:
                file_path = futures[future]
                file = future.result()
                metadata = file.metadata
                result = post_process(file_path, metadata, file)
                results.append(result)
                logger.info(f"Successful ingested file `{file_path}`")
            except Exception as e:
                logger.error(str(e))
        return results
