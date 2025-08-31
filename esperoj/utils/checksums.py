"""
Checksum utilities for calculating file hashes.

This module provides a flexible function for calculating various checksums,
which are essential for verifying file integrity in a digital preservation system.
The function is designed to be memory-efficient by reading files in chunks.
"""

import hashlib
from typing import BinaryIO


def calculate_checksum(file_obj: BinaryIO, algorithm: str = "sha256", chunk_size: int = 8192) -> str:
    """
    Calculates the checksum of a file using a specified algorithm.

    Args:
        file_obj: A binary file-like object to read from.
        algorithm: The hashing algorithm to use (e.g., 'md5', 'sha1', 'sha256', 'sha512').
        chunk_size: The size of chunks to read from the file.

    Returns:
        The checksum as a hex digest.

    Raises:
        ValueError: If the specified algorithm is not supported.
    """
    try:
        hasher = hashlib.new(algorithm)
    except ValueError:
        raise ValueError(f"Unsupported hashing algorithm: {algorithm}")

    # Read directly from the file-like object. The caller is responsible for opening/closing/seeking.
    while chunk := file_obj.read(chunk_size):
        hasher.update(chunk)
    return hasher.hexdigest()
