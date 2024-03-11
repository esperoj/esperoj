"""Storage module."""

from esperoj.storage.s3 import S3Storage
from esperoj.storage.storage import Storage

__all__ = ["Storage", "S3Storage"]
