import hashlib
from os import os
from time import time

import requests
from esperoj.database import Database, Record
from esperoj.storage import Storage
from exiftool import ExifToolHelper


def archive(url: str) -> str:
    api_key = os.environ.get("INTERNET_ARCHIVE_ACCESS_KEY")
    api_secret = os.environ.get("INTERNET_ARCHIVE_SECRET_KEY")

    headers = {
        "Accept": "application/json",
        "Authorization": f"LOW {api_key}:{api_secret}",
    }

    params = {
        "url": url,
        "skip_first_archive": 1,
        "force_get": 1,
        "email_result": 1,
        "delay_wb_availability": 0,
        "capture_screenshot": 0,
    }
    response = requests.post("https://web.archive.org/save", headers=headers, data=params)
    if response.status_code != 200:
        raise RuntimeError(f"Error: {response.text}")
    job_id = response.json()["job_id"]

    start_time = time.time()
    timeout = 300

    while True:
        if time.time() - start_time > timeout:
            raise RuntimeError("Error: Archiving process timed out.")
        response = requests.get(
            f"https://web.archive.org/save/status/{job_id}", headers=headers, timeout=30
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

def calculate_hash(stream, algorithm="sha256") -> str:
    hasher = hashlib.new(algorithm)
    for chunk in stream:
        hasher.update(chunk)
    return hasher.hexdigest()

def calculate_hash_from_url(url: str) -> str:
    response = requests.get(url, stream=True, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(f"Error: {response.text}")
    return calculate_hash(response.iter_content(chunk_size=4096))

class EsperojUtils:
    @staticmethod
    def archive(url: str) -> str:
        return archive(url)

    @staticmethod
    def calculate_hash(stream, algorithm="sha256") -> str:
        return calculate_hash(stream, algorithm)

    @staticmethod
    def calculate_hash_from_url(url: str) -> str:
        return calculate_hash_from_url(url)
