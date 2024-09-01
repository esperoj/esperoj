"""Script to verify daily."""

import concurrent.futures
import time

import requests

from esperoj.database.database import Record


def verify(esperoj, files: list[Record]) -> list[bool]:
    """Verify the integrity of files stored in various locations.

    If any file fails the verification process, a VerificationError is raised with the names
    of the failed files.

    Args:
        esperoj (object): An object containing the necessary databases, storages, and loggers.
        files: list of files to verify.
    """
    logger = esperoj.loggers["Primary"]
    file_hosts = esperoj.config["file_hosts"]

    def verify_file(file) -> bool:
        """Verify the integrity of a single file.

        Args:
            file (dict): A dictionary containing the file metadata.

        Returns:
            bool: True if the file verification succeeded, False otherwise.
        """
        name = file["Name"]
        calculate_hash = esperoj.utils.calculate_hash

        def calculate_hash_from_storage_name(storage_name):
            return calculate_hash(esperoj.storages[storage_name].get_file(name))

        def calculate_hash_from_url(url):
            return calculate_hash(requests.get(url, stream=True, timeout=30).iter_content(2**20))

        def get_size_from_url(url):
            return int(requests.head(url, timeout=60).headers["content-length"])

        try:
            start_time = time.time()
            logger.info(f"Start verifying file `{name}`")
            result = False
            if file["Verified"]:
                size_list = [esperoj.storages[storage_name].size(name) for storage_name in file["Storages"]]
                size_list.append(file["Size"])
                urls = [file[host] for host in file_hosts]
                urls.append(file["Internet Archive"])
                size_list = size_list + [get_size_from_url(url) for url in urls]
                if len(set(size_list)) == 1:
                    result = True
            else:
                with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                    hash_list = [file["SHA256"]]
                    urls = [file[host] for host in file_hosts]
                    urls.append(file["Internet Archive"])
                    futures = [
                        executor.submit(calculate_hash_from_storage_name, storage_name)
                        for storage_name in file["Storages"]
                    ]
                    futures = futures + [executor.submit(calculate_hash_from_url, url) for url in urls]
                    for future in concurrent.futures.as_completed(futures):
                        hash_list.append(future.result())
                    if len(set(hash_list)) == 1:
                        result = True
            if not file["Verified"]:
                file["Verified"] = True
            logger.info(f"Verified file `{name}` in {time.time() - start_time} seconds")
            return result
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        results = []
        failed_files = []
        futures = {executor.submit(verify_file, file): file for file in files}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if not result:
                failed_files.append(futures[future]["Name"])
            results.append(result)
        if failed_files:
            logger.info(f"Verification failed for the following files: {', '.join(failed_files)}")
    return results
