"""Module contain utils."""

import hashlib
from typing import BinaryIO


def calculate_hash(stream: BinaryIO, algorithm: str = "sha256") -> str:
    hasher = hashlib.new(algorithm)
    for chunk in iter(lambda: stream.read(2**20), b""):
        hasher.update(chunk)
    return hasher.hexdigest()
