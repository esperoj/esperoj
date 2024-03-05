"""Module that contains the Esperoj class, which can ingest and archive files."""

import json
import logging
import os
from pathlib import Path

from exiftool import ExifToolHelper

from esperoj.database import Database, Record
from esperoj.storage import Storage
from esperoj.utils import archive, calculate_hash, calculate_hash_from_url, get_db


class Esperoj:
    """The Esperoj class.

    Attributes:
        db (Database | None): The database object.
        storage (Storage | None): The storage object.
        logger (logging.Logger | None): The logger to log.
    """

    def __init__(
        self,
        db: Database | None = None,
        storage: Storage | None = None,
        logger: logging.Logger | None = None,
    ):
        """Initialize the Esperoj object with the given database and storage.

        Args:
            db (Database | None): The database object that stores the file records.
            storage (Storage | None): The storage object that handles the file upload and download.
            logger (logging.Logger | None): The logger to log.
        """
        if db is None:
            if os.environ.get("ESPEROJ_DATABASE") == "Airtable":
                db = get_db("Airtable")
            if os.environ.get("ESPEROJ_DATABASE") == "Seatable":
                db = get_db("Seatable")
            else:
                db = get_db("Memory")
        if storage is None:
            from esperoj.storage.s3 import S3Storage

            storage = S3Storage(
                name="S3",
                config={
                    "client_config": {
                        "aws_access_key_id": os.getenv("S3_ACCESS_KEY_ID"),
                        "aws_secret_access_key": os.getenv("S3_SECRET_ACCESS_KEY"),
                        "endpoint_url": os.getenv("S3_ENDPOINT_URL"),
                    },
                    "bucket_name": "hlongn",
                },
            )
        if logger is None:
            logger = logging.getLogger("Esperoj")
            logger.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        self.db = db
        self.logger = logger
        self.storage = storage

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
        archive_url = archive(url)
        record.update({"Internet Archive": archive_url})
        return archive_url

    def ingest(self, path: Path) -> Record:
        """Ingest the file at the given path into the database and the storage.

        This method ingests the file, extracts metadata, and creates a record in the
        "Files" table. If the file is a music file (FLAC, MP3, or M4A), it also creates
        a record in the "Musics" table.

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
        metadata, sha256sum = "", ""
        with path.open("rb") as f, ExifToolHelper() as et:
            sha256sum = calculate_hash(f, algorithm="sha256")
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

        title_key, artist_key = "", ""
        match path.suffix:
            case ".flac":
                title_key, artist_key = "Vorbis:Title", "Vorbis:Artist"
            case ".mp3":
                title_key, artist_key = "ID3:Title", "ID3:Artist"
            case ".m4a":
                title_key, artist_key = "QuickTime:Title", "QuickTime:Artist"

        if title_key and artist_key:
            title = metadata[0].get(title_key, "Unknown Title")
            artist = metadata[0].get(artist_key, "Unknown Artist")
            self.db.table("Musics").create(
                {
                    "Name": title,
                    "Artist": str(artist),
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
        storage_hash = calculate_hash_from_url(self.storage.get_link(fields["Name"]))
        if archive_url == "":
            archive_url = self.archive(record_id)
        archive_hash = calculate_hash_from_url(archive_url)
        return storage_hash == archive_hash == fields["SHA256"]
