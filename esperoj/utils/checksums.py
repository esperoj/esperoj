"""
Checksum utilities for calculating file hashes.

This module provides functions for calculating common checksums like MD5 and SHA256,
which are essential for verifying file integrity in a digital preservation system.
The functions are designed to be memory-efficient by reading files in chunks.
"""

import hashlib
from pathlib import Path


def calculate_md5(file_path: Path, chunk_size: int = 8192) -> str:
    """
    Calculates the MD5 checksum of a file.

    Args:
        file_path: The path to the file.
        chunk_size: The size of chunks to read from the file.

    Returns:
        The MD5 checksum as a hex digest.
    """
    md5 = hashlib.md5()
    with file_path.open("rb") as f:
        while chunk := f.read(chunk_size):
            md5.update(chunk)
    return md5.hexdigest()


def calculate_sha256(file_path: Path, chunk_size: int = 8192) -> str:
    """
    Calculates the SHA256 checksum of a file.

    Args:
        file_path: The path to the file.
        chunk_size: The size of chunks to read from the file.

    Returns:
        The SHA256 checksum as a hex digest.
    """
    sha256 = hashlib.sha256()
    with file_path.open("rb") as f:
        while chunk := f.read(chunk_size):
            sha256.update(chunk)
    return sha256.hexdigest()


def calculate_checksums(file_path: Path) -> dict[str, str]:
    """
    Calculates multiple checksums (MD5, SHA256) for a file in a single pass.

    Args:
        file_path: The path to the file.

    Returns:
        A dictionary containing the 'md5' and 'sha256' checksums.
    """
    md5 = hashlib.md5()
    sha256 = hashlib.sha256()
    chunk_size = 8192

    with file_path.open("rb") as f:
        while chunk := f.read(chunk_size):
            md5.update(chunk)
            sha256.update(chunk)

    return {"md5": md5.hexdigest(), "sha256": sha256.hexdigest()}
