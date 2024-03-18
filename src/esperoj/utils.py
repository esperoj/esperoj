"""Module contain utils."""

import hashlib
from typing import Iterator


def calculate_hash(stream: Iterator, algorithm: str = "sha256") -> str:
    hasher = hashlib.new(algorithm)
    for chunk in stream:
        hasher.update(chunk)
    return hasher.hexdigest()
