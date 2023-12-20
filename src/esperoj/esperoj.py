"""Module contain Esperoj class."""
import hashlib
from pathlib import Path


class Esperoj:
    """Esperoj class for handling file ingestion and information retrieval.

    Attributes
    ----------
        db: A database object.
        storage: A storage object.
    """

    def __init__(self, db, storage):
        """Initialize Esperoj with a database and storage.

        Args:
            db: A database object.
            storage: A storage object.
        """
        self.db = db
        self.storage = storage

    def calculate_hash(self, content=None, path=None, algorithm="sha256"):
        """Calculate hash from either content or a file path.

        Args:
            content (str, optional): Content to calculate hash from. Defaults to None.
            path (str, optional): Path to a file to calculate hash from. Defaults to None.
            algorithm (str, optional): Hashing algorithm to use. Defaults to "sha256".

        Raises
        ------
            ValueError: If neither content nor path is provided.
            ValueError: If the provided algorithm is not supported.

        Returns
        -------
            str: The calculated hash.
        """
        if content is None and path is None:
            raise ValueError("Either content or path must be provided")
        if algorithm not in hashlib.algorithms_available:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
        hash_obj = hashlib.new(algorithm)
        if content is not None:
            hash_obj.update(content.encode())
        else:
            if not isinstance(path, Path):
                path = Path(path)
            with path.open("rb") as f:
                hash_obj.update(f.read())
        return hash_obj.hexdigest()

    def ingest(self, path: Path) -> dict:
        """Ingests a file into the database and storage.

        Args:
            path (Path): Path to the file to ingest.

        Raises
        ------
            FileNotFoundError: If the provided path does not point to a file.
            FileExistsError: If a file with the same name already exists in the database or storage.

        Returns
        -------
            dict: A dictionary containing information about the ingested file.
        """
        if not path.is_file():
            raise FileNotFoundError("Invalid file path. Please enter a valid file path.")

        files = self.db.table("Files")
        name = path.name

        if len(files.match({"Name": name})) > 0:
            raise FileExistsError("File exists.")

        if self.storage.file_exists(name):
            raise FileExistsError("File exists.")

        size = path.stat().st_size
        sha256sum = self.calculate_hash(path=path, algorithm="sha256")

        self.storage.upload_file(str(path), name)

        return files.create(
            {"Name": name, "Size": size, "SHA256": sha256sum, self.storage.name: name}
        )
