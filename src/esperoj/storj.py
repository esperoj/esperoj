"""Module contain Storj class."""
import os
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig

from .storage import Storage


class Storj(Storage):
    """Storj class."""

    def __init__(self):
        self.client = boto3.client(
            "s3",
            aws_access_key_id=os.environ["STORJ_ACCESS_KEY"],
            aws_secret_access_key=os.environ["STORJ_SECRET_KEY"],
            endpoint_url=os.environ["STORJ_ENDPOINT_URL"],
        )
        self.bucket_name = "esperoj"
        self.config = TransferConfig(
            multipart_threshold=64 * 2**20, max_concurrency=8, multipart_chunksize=64 * 2**20
        )

    def upload_file(self, path: Path, name: str):
        """Upload a file to storage."""
        self.client.upload_file(str(path), self.bucket_name, name, Config=self.config)
