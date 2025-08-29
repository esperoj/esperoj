"""
The `esperoj` package provides a flexible and scalable file storage solution
with multi-backend support, built on fsspec and Django ORM.

It exposes custom fsspec file systems for various storage backends,
and a central `EsperojFileSystem` that manages file metadata and replicas
across these backends.
"""

from .storages import CatboxFileSystem
from .storages import EsperojFileSystem
from .storages.config import esperoj_fs

__all__ = ["CatboxFileSystem", "EsperojFileSystem", "esperoj_fs"]
