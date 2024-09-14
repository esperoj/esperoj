"""Module containing utility functions."""

import hashlib
from collections.abc import Iterator


def calculate_hash(stream: Iterator, algorithm: str = "sha256") -> str:
    """Calculate the hash of a stream of data using the specified algorithm.

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


class Utils:
    def __getattr__(self, name: str):
        """Get util from this package.

        Args:
            name (str): The name of the util.

        Returns:
            callable: The imported method, or None if the import fails.
        """
        match name:
            case "calculate_hash":
                return calculate_hash
            case "ingest":
                from esperoj.utils.ingest import ingest

                return ingest

            case "verify":
                from esperoj.utils.verify import verify

                return verify
            case _:
                raise AttributeError(f"Util {name} does not exist.")
