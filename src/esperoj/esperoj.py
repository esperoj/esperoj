import hashlib
from pathlib import Path


class Esperoj:
    def __init__(self, db, storage):
        self.db = db
        self.storage = storage

    def calculate_hash(self, content=None, path=None, algorithm="sha256"):
        # Check if the content or the path is given
        if content is None and path is None:
            raise ValueError("Either content or path must be provided")
        # Check if the algorithm is supported
        if algorithm not in hashlib.algorithms_available:
            raise ValueError(f"Unsupported algorithm: {self.algorithm}")
        # Create a hash object
        hash_obj = hashlib.new(algorithm)
        # Update the hash object with the content or the file data
        if content is not None:
            hash_obj.update(content.encode())
        else:
            # Convert the path to a Path object if it is not already
            if not isinstance(path, Path):
                path = Path(path)
            # Read the file data in binary mode and update the hash object
            with path.open("rb") as f:
                hash_obj.update(f.read())
        # Store the hexadecimal digest of the hash object
        hash = hash_obj.hexdigest()
        return hash

    def info(self):
        print(self.__dict__)
        print(self.storage.client.list_buckets())
        print(self.db.table("Files").all())

    def ingest(self, path: Path):
        if path.exists() and path.is_file():
            name = path.name
            size = path.stat().st_size
            hash = self.calculate_hash(path=path, algorithm="sha256")
            self.storage.upload_file(path, name)
            self.db.table("Files").create(
                {"Name": name, "Size": size, "SHA256": hash, "Storj": name}
            )
        else:
            print("Invalid file path. Please enter a valid file path.")
