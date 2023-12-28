"""Module that contains the Esperoj class, which can ingest and archive files."""

import hashlib
import json
import os
import time
from collections.abc import Iterator
from pathlib import Path

import requests
import logging
import logging
import logging
from src.esperoj.logger import logger

from exiftool import ExifToolHelper

from esperoj.database import Database, Record
from esperoj.storage import Storage


class Esperoj:
    """The Esperoj class.

    Attributes:
        db (Database | None): The database object.
        logger (logging.Logger): The logger object.
        logger (logging.Logger): The logger object.
        storage (Storage | None): The storage object.
        logger (logging.Logger): The logger object.
        storage (Storage | None): The storage object.
        logger (logging.Logger): The logger object.
    """

    def __init__(self, db: Database | None = None, storage: Storage | None = None):
        """Initialize the Esperoj object with the given database and storage.

        Args:
            db (Database | None): The database object that stores the file records.
            storage (Storage | None): The storage object that handles the file upload and download.
        """
        if db is not None:
            self.db = db
        elif os.environ.get("ESPEROJ_DATABASE") == "Airtable":
            from esperoj.database.airtable import Airtable

            self.db = Airtable("Airtable")
        else:
            from esperoj.database.memory import MemoryDatabase

            self.db = MemoryDatabase("Memory Database")
        if storage is not None:
            self.storage = storage
        else:
            from esperoj.storage.s3 import S3Storage

            self.storage = S3Storage(
                name="Storj",
                config={
                    "client_config": {
                        "aws_access_key_id": os.getenv("STORJ_ACCESS_KEY_ID"),
                        "aws_secret_access_key": os.getenv("STORJ_SECRET_ACCESS_KEY"),
                        "endpoint_url": os.getenv("STORJ_ENDPOINT_URL"),
                    },
                    "bucket_name": "esperoj",
                },
            )

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
                f"https://web.archive.org/save/status/{job_id}", headers=headers, timeout=30
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
    def _calculate_hash(stream: Iterator, algorithm: str = "sha256") -> str:
        """Calculate the hash of the given data using the specified algorithm.

        Args:
            stream (Iterator): The data to be hashed.
            algorithm (str, optional): The name of the hashing algorithm. Defaults to "sha256".

        Returns:
            str: The hexadecimal representation of the hash.
        """
        hasher = hashlib.new(algorithm)
        for chunk in stream:
            hasher.update(chunk)
        return hasher.hexdigest()

    @staticmethod
    def _calculate_hash_from_url(url: str) -> str:
        """Calculate the hash of the file at the given URL.

        Args:
            url (str): The URL of the file to be hashed.

        Returns:
            str: The hexadecimal representation of the hash.

        Raises:
            RuntimeError: If the file at the given URL cannot be accessed.
        """
        response = requests.get(url, stream=True, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(f"Error: {response.text}")
        return Esperoj._calculate_hash(response.iter_content(chunk_size=4096))

    def archive(self, record_id: str) -> str:
        """Archive the file with the given ID using the savepagenow service.

        Args:
            record_id (str): The ID of the file to be archived.

        Returns:
            url (str): The URL of the archived file.

        Raises:
            RuntimeError: If the file cannot be found in the database or the storage, or if the archived version of the file cannot be retrieved.
            RecordNotFoundError: If can't get the file from database.
        """
        record = self.db.table("Files").get(record_id)
        url = self.storage.get_link(record.fields["Name"])
        archive_url = Esperoj._archive(url)
        record.update({"Internet Archive": archive_url})
        return archive_url

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
        metadata = ""
        sha256sum = ""
        with path.open("rb") as f, ExifToolHelper() as et:
            sha256sum = Esperoj._calculate_hash(f, algorithm="sha256")
            metadata = et.get_metadata(str(path))
        files = self.db.table("Files")

        if self.storage.file_exists(name) or (
            next(files.get_all({"Name": name}), None) is not None
        ):
            raise FileExistsError

        self.storage.upload_file(str(path), name)
        record = files.create(
            {
                "Name": name,
                "Size": size,
                "SHA256": sha256sum,
                self.storage.name: name,
                "Metadata": json.dumps(metadata),
            }
        )
        match path.suffix:
            case ".flac":
                title = metadata[0].get("Vorbis:Title", "Unknown Title")
                artist = metadata[0].get("Vorbis:Artist", "Unknown Artist")
                self.db.table("Musics").create(
                    {
                        "Name": title,
                        "Artist": artist,
                        "Files": [record.record_id],
                    }
                )
        return record

    def verify(self, record_id: str) -> bool:
        """Verify the integrity of the file with the given ID by comparing the SHA256 checksums of the file and its archived version.

        Args:
            record_id (str): The ID of the file to be verified.

        Returns:
            bool: True if the file is intact, False otherwise.

        Raises:
            FileNotFoundError: If the file cannot be found in the storage.
            RecordNotFoundError: If the file cannot be found in the database.
        """
        fields = self.db.table("Files").get(record_id).fields
        archive_url = fields.get("Internet Archive", "")
        if archive_url == "":
            archive_url = self.archive(record_id)
        return (
            Esperoj._calculate_hash_from_url(self.storage.get_link(fields["Name"]))
            == Esperoj._calculate_hash_from_url(archive_url)
            == fields["SHA256"]
        )
