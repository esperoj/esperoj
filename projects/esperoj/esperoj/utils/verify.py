"""Script to verify daily."""

import concurrent.futures
import time

from esperoj.database.models import File


def verify(esperoj, files: list[File]) -> list[bool]:
    """Verify the integrity of files stored in various locations.

    If any file fails the verification process, a VerificationError is raised with the names
    of the failed files.

    Args:
        esperoj (object): An object containing the necessary databases, storages, and loggers.
        files: list of files to verify.
    """
    logger = esperoj.loggers["primary"]
    file_hosts = esperoj.file_hosts
    storages = esperoj.storages
    files_table = esperoj.databases["primary"].get_table("files")
    results = []
    failed_files = []
    update_to_verified = []

    def verify_file(file: File) -> bool:
        """Verify the integrity of a single file.

        Args:
            file (dict): A dictionary containing the file metadata.

        Returns:
            bool: True if the file verification succeeded, False otherwise.
        """
        name = file.name
        calculate_hash = esperoj.utils.calculate_hash

        def calculate_hash_from_mirror_info(mirror_name, mirror_info):
            if mirror_name in esperoj.storages:
                return calculate_hash(storages[mirror_name].stream(mirror_info["sources"][0]))
            return calculate_hash(file_hosts[mirror_name].stream(mirror_info["sources"][0]))

        try:
            start_time = time.time()
            logger.info(f"Start verifying file `{name}`")
            result = False
            with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
                hash_list = [file.sha256]
                futures = [
                    executor.submit(calculate_hash_from_mirror_info, mirror_name, mirror_info)
                    for mirror_name, mirror_info in file.mirrors.items()
                ]
                for future in concurrent.futures.as_completed(futures):
                    hash_list.append(future.result())
                if len(set(hash_list)) == 1:
                    result = True
            logger.info(f"Verified file `{name}` in {time.time() - start_time:.2f} seconds")
            return result
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(verify_file, file): file for file in files}
        for future in concurrent.futures.as_completed(futures):
            file = futures[future]
            result = future.result()
            if not result:
                failed_files.append(futures[future].name)
            else:
                if not file.verified:
                    update_to_verified.append({"id": file.id, "verified": True})
            results.append(result)
        if failed_files:
            logger.info(f"Verification failed for the following files: {', '.join(failed_files)}")
    if update_to_verified != []:
        files_table.batch_update(update_to_verified)
    return results
