"""Ingest util."""
from typing import Callable
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from esperoj.database.database import Record
from esperoj.utils import calculate_hash

def ingest(esperoj, path: Path, storage_names: list[str], post_process: Callable[[Path, dict, Record], Record]) -> list[Record]:
    """Ingest a file into the Esperoj system.

    Args:
        esperoj (object): The Esperoj object representing the system.
        path (Path): The path to be ingested.

    Returns:
        list(Record): The database records representing the ingested files.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        FileExistsError: If the file already exists in the system.
        RuntimeError: If the file type is not supported.
    """
    logger = esperoj.loggers["Primary"]

    file_paths = []

    if path.is_dir():
        file_paths = [file_path for file_path in path.iterdir() if file_path.is_file()]
    else:
        if not path.is_file():
            raise FileNotFoundError
        file_paths = [path]

    def ingest_file(file_path: Path) -> Record:
        logger.info(f"Start to ingest file `{file_path!s}`")

        file_hosts = esperoj.config["file_hosts"]
        name = file_path.name
        size = file_path.stat().st_size
        f = file_path.open("rb")
        sha256sum = calculate_hash(f, algorithm="sha256")
        f.close()
        metadata = json.loads(
            subprocess.check_output(["exiftool", "-j", str(file_path)])
        )[0]
        files = esperoj.databases["Primary"].get_table("Files")

        def upload() -> Record:
            """Upload the file to the storages, and file hosts, then return a database record for it.

            Returns:
                Record: The database record representing the ingested file.

            Raises:
                FileExistsError: If the file already exists in any of the storages or database.
            """
            if list(filter(lambda file: file["Name"] == name, files.query())) != []:
                raise FileExistsError
            file = files.create(
                {
                    "Name": name,
                    "Size": size,
                    "SHA256": sha256sum,
                    "Internet Archive": "https://example.com/",
                    "Verified": False,
                    "Storages": storage_names,
                    "Metadata": json.dumps(metadata),
                }
            )
            for storage_name in storage_names:
                storage = esperoj.storages[storage_name]
                if storage.file_exists(name):
                    raise FileExistsError
                try:
                    storage.upload_file(str(file_path), name)
                    uploaded_storages.append(storage_name)
            results = esperoj.utils.share(str(file_path), name, file_hosts)
            for host, result in results:
                if not isinstance(result, Exception):
                    file[host] = result
            return file

        file = upload(["Audio Storage", "Backup Audio Storage"])
        url = file[file_hosts[0]]
        archive_url = esperoj.save_page(url)
        file.update({"Internet Archive": archive_url})
        return file

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = []
        futures = {
            executor.submit(ingest_file, file_path): file_path
            for file_path in file_paths
        }
        for future in as_completed(futures):
            try:
                results.append(future.result())
                logger.info(f"Successful ingested file `{future_to_file_path[future]}`")
            except Exception as e:
                logger.error(str(e))
        return results
