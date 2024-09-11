from esperoj.storage.storage import DeleteFilesResponse, Storage
from esperoj.database.database import Database

class EsperojStorage():
    def __init__(self, database: Database, storage: Storage) -> None:
        self.storage = storage
        self.database = database

    def delete_files(self, paths: list[str]) -> None:
        self.storage.delete_files(paths)
    def download_file(self, src: str, dst: str) -> None:
        """Download a file from the S3 bucket.

        Args:
            src (str): The path of the file to download.
            dst (str): The destination path where the file will be saved.

        Raises:
            ClientError: If an error occurs while downloading the file.
        """
        self.client.download_file(self.config["bucket_name"], src, dst, Config=self.config["transfer_config"])

    def file_exists(self, path: str) -> bool:
        """Check if a file exists in the S3 bucket.

        Args:
            path (str): The path of the file to check.

        Returns:
            bool: True if the file exists, False otherwise.

        Raises:
            ClientError: If an error occurs while checking for the file's existence.
        """
        try:
            self.client.head_object(Bucket=self.config["bucket_name"], Key=path)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise e

    def get_link(self, path: str) -> str:
        """Get a pre-signed URL for a file in the S3 bucket.

        Args:
            path (str): The path of the file to get the URL for.

        Returns:
            str: A pre-signed URL for the file.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        if not self.file_exists(path):
            raise FileNotFoundError(f"No such file: '{path}'")
        return self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.config["bucket_name"], "Key": path},
            ExpiresIn=3600 * 24 * 7,
        )

    def get_file(self, src: str) -> Iterator:
        """Get a file from the S3 bucket and return an Iterator.

        Args:
            src (str): The path of the file to download.

        Returns:
            Iterator: An Iterator of the file content.

        Raises:
            ClientError: If an error occurs while downloading the file.
        """
        return self.client.get_object(Bucket=self.config["bucket_name"], Key=src)["Body"].iter_chunks(2**20)

    def list_files(self, path: str) -> list:
        """List all files in the specified path of the S3 bucket.

        Args:
            path (str): The path to list files from.

        Returns:
            list[str]: A list of file paths.

        Raises:
            FileNotFoundError: If the specified path does not exist.
        """
        paginator = self.client.get_paginator("list_objects_v2")
        files: list[str] = []
        for page in paginator.paginate(Bucket=self.config["bucket_name"], Prefix=path):
            files.extend(obj["Key"] for obj in page.get("Contents", []))
        if not files:
            raise FileNotFoundError(f"No such directory: '{path}'")
        return files

    def upload_file(self, src: str, dst: str) -> None:
        """Upload a file to the S3 bucket.

        Args:
            src (str): The source path of the file to upload.
            dst (str): The destination path in the S3 bucket.

        Raises:
            ClientError: If an error occurs while uploading the file.
            FileNotFoundError: If the source file does not exist.
        """
        try:
            self.client.upload_file(src, self.config["bucket_name"], dst, Config=self.config["transfer_config"])
        except ClientError as e:
            raise e
        except FileNotFoundError as e:
            raise FileNotFoundError(f"No such file: '{src}'") from e

    def size(self, src: str) -> int:
        """Check file size

        Args:
            src (str): The path of the file.

        Returns:
            size (int): Size of the object.
        """
        return self.client.head_object(Bucket=self.config["bucket_name"], Key=src)["ContentLength"]
