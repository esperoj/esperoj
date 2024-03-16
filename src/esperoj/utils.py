"""Module contain utils."""

import hashlib


def calculate_hash(stream, algorithm: str = "sha256") -> str:
    hasher = hashlib.new(algorithm)
    for chunk in stream:
        hasher.update(chunk)
    return hasher.hexdigest()
