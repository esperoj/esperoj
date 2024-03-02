"""Module contain utils."""
import hashlib
import os
import time
from collections.abc import Iterator

import requests
from requests.adapters import HTTPAdapter, Retry

from esperoj.database import Database


def archive(url: str) -> str:
    """Archive a URL using the Save Page Now 2 (SPN2) API.

    Args:
        url (str): The URL to archive.

    Returns:
        url (str): The archived URL.

    Raises:
        RuntimeError: If the URL cannot be archived or if a timeout occurs.
    """
    api_key = os.environ.get("INTERNET_ARCHIVE_ACCESS_KEY")
    api_secret = os.environ.get("INTERNET_ARCHIVE_SECRET_KEY")

    headers = {
        "Accept": "application/json",
        "Authorization": f"LOW {api_key}:{api_secret}",
    }

    params = {
        "url": url,
        "capture_all": 0,
        "capture_outlinks": 0,
        "capture_screenshot": 0,
        "delay_wb_availability": 0,
        "force_get": 1,
        "skip_first_archive": 1,
        "outlinks_availability": 0,
        "email_result": 1,
        "js_behavior_timeout": 0,
    }

    session = requests.Session()

    retry_strategy = Retry(
        total=2,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    response = session.post(
        "https://web.archive.org/save", headers=headers, data=params, timeout=12
    )
    if response.status_code != 200:
        raise RuntimeError(f"Error: {response.text}")
    job_id = response.json()["job_id"]

    start_time = time.time()
    timeout = 60 * 15

    while True:
        if time.time() - start_time > timeout:
            raise RuntimeError("Error: Archiving process timed out.")
        response = session.get(
            f"https://web.archive.org/save/status/{job_id}", headers=headers, timeout=12
        )
        if response.status_code != 200:
            raise RuntimeError(f"Error: {response.text}")
        status = response.json()
        match status["status"]:
            case "pending":
                time.sleep(5)
            case "success":
                return f'https://web.archive.org/web/{status["timestamp"]}/{status["original_url"]}'
            case _:
                raise RuntimeError(f"Error: {response.text}")


def calculate_hash(stream: Iterator, algorithm: str = "sha256") -> str:
    """Calculate the hash of the given data using the specified algorithm.

    Args:
        stream (Iterator): The data to be hashed.
        algorithm (str, optional): The name of the hashing algorithm. Defaults to "sha256".

    Returns:
        str: The hexadecimal representation of the hash.
    """
    hasher = hashlib.new(algorithm)
    for chunk in stream:
        hasher.update(chunk)
    return hasher.hexdigest()


def calculate_hash_from_url(url: str) -> str:
    """Calculate the hash of the file at the given URL.

    Args:
        url (str): The URL of the file to be hashed.

    Returns:
        str: The hexadecimal representation of the hash.

    Raises:
        RuntimeError: If the file at the given URL cannot be accessed.
    """
    response = requests.get(url, stream=True, timeout=60)
    if response.status_code != 200:
        raise RuntimeError(f"Error: {response.text}")
    return calculate_hash(response.iter_content(chunk_size=4096))


def get_db(name: str) -> Database:
    """Get the database by name.

    Args:
        name (str): The name of the database to get.

    Returns:
        Database: The database instance.

    """
    if name == "Airtable":
        from esperoj.database.airtable import Airtable

        return Airtable(name)

    from esperoj.database.memory import MemoryDatabase

    return MemoryDatabase(name)
