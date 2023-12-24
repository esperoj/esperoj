"""Module that contains the Esperoj class, which can ingest and archive files."""

import hashlib
import os
import time
from pathlib import Path

import requests

from esperoj.database import Record
from esperoj.database.airtable import Airtable
from esperoj.database.memory import MemoryDatabase
from esperoj.storage.s3 import S3Storage


class Esperoj:
    """A class that handles the ingestion and archiving of files using a database and a storage service.

    The Esperoj class can ingest files from a local path and store them in a database and a storage service. It can also archive files using the savepagenow service, which captures a snapshot of a web page and returns its URL.

    Attributes:
        db (Airtable | MemoryDatabase): The database object that stores the file records. It can be either an Airtable or a MemoryDatabase object, which are subclasses of the abstract Database class.
        storage (S3Storage): The storage object that handles the file upload and download. It is an S3Storage object that uses Amazon S3 as the storage service.
    """

    def __init__(self, db: Airtable | MemoryDatabase, storage: S3Storage) -> None:
        """Initialize the Esperoj object with the given database and storage.

        Args:
            db (Airtable | MemoryDatabase): The database object that stores the file records. It can be either an Airtable or a MemoryDatabase object, which are subclasses of the abstract Database class.
            storage (S3Storage): The storage object that handles the file upload and download. It is an S3Storage object that uses Amazon S3 as the storage service.
        """
        self.db = db
        self.storage = storage

    @staticmethod
    def _archive(url: str) -> str:
        """Archive a URL using the Save Page Now 2 (SPN2) API.

        Args:
            url (str): The URL to archive.

        Returns:
            url (str): The archived URL.

        Raises:
            RuntimeError: If the URL cannot be archived or if a timeout occurs.
        """
        api_key = os.environ.get("INTERNET_ARCHIVE_ACCESS_KEY")
        api_secret = os.environ.get("INTERNET_ARCHIVE_SECRET_KEY")

        headers = {
            "Accept": "application/json",
            "Authorization": f"LOW {api_key}:{api_secret}",
        }

        params = {
            "url": url,
            "skip_first_archive": 1,
            "force_get": 1,
            "email_result": 1,
            "delay_wb_availability": 0,
            "capture_screenshot": 0,
        }
        response = requests.post("https://web.archive.org/save", headers=headers, data=params)
        if response.status_code != 200:
            raise RuntimeError(f"Error: {response.text}")
        job_id = response.json()["job_id"]

        start_time = time.time()
        timeout = 300

        while True:
            if time.time() - start_time > timeout:
                raise RuntimeError("Error: Archiving process timed out.")
            response = requests.get(
                f"https://web.archive.org/save/status/{job_id}", headers=headers
            )
            if response.status_code != 200:
                raise RuntimeError(f"Error: {response.text}")
            status = response.json()
            match status["status"]:
                case "pending":
                    time.sleep(5)
                case "success":
                    return f'https://web.archive.org/web/{status["timestamp"]}/{status["original_url"]}'
                case _:
                    raise RuntimeError(f"Error: {response.text}")

    @staticmethod
    def _calculate_hash(content: bytes, algorithm: str = "sha256") -> str:
        """Calculate the hash of the given content using the specified algorithm.

        Args:
            content (bytes): The content to be hashed.
            algorithm (str, optional): The name of the hashing algorithm. Defaults to "sha256".

        Returns:
            str: The hexadecimal representation of the hash.
        """
        hasher = hashlib.new(algorithm)
        hasher.update(content)
        return hasher.hexdigest()

    def archive(self, name: str) -> Record:
        """Archive the file with the given name using the savepagenow service.

        Args:
            name (str): The name of the file to be archived.

        Returns:
            record (Record): The new record with archive url.

        Raises:
            FileNotFoundError: If the file is not in the repository.
        """
        record = next(self.db.table("Files").get_all({"Name": name}), None)
        if not record:
            raise FileNotFoundError
        url = self.storage.get_link(name)
        archive_url = Esperoj._archive(url)
        return record.update({"Internet Archive": archive_url})

    def ingest(self, path: Path) -> Record:
        """Ingest the file at the given path into the database and the storage.

        Args:
            path (Path): The path of the file to be ingested.

        Returns:
            Record: The record object that represents the file in the database.

        Raises:
            FileNotFoundError: If the path is not a valid file.
            FileExistsError: If the file already exists in the database or the storage.
        """
        if not path.is_file():
            raise FileNotFoundError

        name = path.name
        size = path.stat().st_size
        sha256sum = Esperoj._calculate_hash(path.read_bytes(), algorithm="sha256")
        files = self.db.table("Files")

        if self.storage.file_exists(name) or next(files.get_all({"Name": name}), None):
            raise FileExistsError

        self.storage.upload_file(str(path), name)

        return files.create(
            {"Name": name, "Size": size, "SHA256": sha256sum, self.storage.name: name}
        )
