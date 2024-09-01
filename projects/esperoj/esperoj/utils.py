"""Module containing utility functions."""

import concurrent.futures
import hashlib
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import quote

import requests


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


class UploadError(Exception):
    def __init__(self, host, status_code=None, server_message=None, message="Failed to upload"):
        self.host = host
        self.status_code = status_code
        self.server_message = server_message
        super().__init__(f"{message} to {host}. Status: {status_code}. Server message: {server_message}")


def share(path: str, file_name: str | None = None, file_hosts: list[str] | None = None) -> dict[str, str | UploadError]:
    """Share a file to file hosts.

    Args:
    path (str): A file path to upload.
    file_name (str): The name of the file.
    file_hosts (list[str]): List of file hosts to upload.

    Returns:
    results (dict[str, str | UploadError]): The results with key being file host and value being direct URL or UploadError.
    """

    file_path = Path(path)
    if file_hosts is None:
        file_hosts = ["lain_la", "file_haus"]
    if file_name is None:
        file_name = file_path.name

    def upload_to_lain_la() -> str:
        url = "https://pomf.lain.la/upload.php"
        with file_path.open("rb") as file:
            files = {"files[]": (file_name, file)}
            response = requests.post(url, files=files, timeout=600)
            if response.status_code == 200:
                json_response = response.json()
                return json_response["files"][0]["url"]
            raise UploadError("lain_la", response.status_code, response.text)

    def upload_to_file_haus() -> str:
        encoded_file_name = quote(file_name)
        url = f"https://filehaus.top/api/upload/{encoded_file_name}"
        with file_path.open("rb") as file:
            response = requests.put(url, data=file, timeout=600)
            if response.status_code == 200:
                return response.text
            raise UploadError("file_haus", response.status_code, response.text)

    upload_functions = {"lain_la": upload_to_lain_la, "file_haus": upload_to_file_haus}

    results = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(upload_functions[host]): host for host in file_hosts if host in upload_functions}
        for future in concurrent.futures.as_completed(futures):
            host = futures[future]
            try:
                url = future.result()
                results[host] = url
            except UploadError as e:
                results[host] = e

    return results
