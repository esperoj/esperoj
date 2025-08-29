"""
This module contains custom storage backends for the esperoj application,
including fsspec-compatible file systems.
"""

from .catbox import CatboxFileSystem
from .esperoj import EsperojFileSystem

__all__ = ["CatboxFileSystem", "EsperojFileSystem"]
