"""Module containing utility functions."""

import hashlib
from typing import Iterator


def calculate_hash(stream: Iterator, algorithm: str = "sha256") -> str:
    """
    Calculate the hash of a stream of data using the specified algorithm.

    Args:
        stream (Iterator): An iterator that yields the data to be hashed.
        algorithm (str): The name of the hashing algorithm to use (e.g., "sha256", "md5").

    Returns:
        str: The hexadecimal digest of the hashed data.
    """
    hasher = hashlib.new(algorithm)
    for chunk in stream:
        hasher.update(chunk)
    return hasher.hexdigest()
