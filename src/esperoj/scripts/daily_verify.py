"""Script to verify daily."""

import concurrent.futures
import datetime
import time
from functools import partial
from esperoj.utils import calculate_hash
import requests
import os


class VerificationError(Exception):
    """Raised when the verification of one or more files fails."""

    pass


def daily_verify(esperoj) -> None:
    """Verify the integrity of files stored in various locations.

    This function retrieves a list of files from the "Files" table in the "Primary" database.
    It then verifies that the hash of the file stored in the primary storage, backup storage,
    and Internet Archive matches the expected SHA256 hash stored in the database.

    If any file fails the verification process, a VerificationError is raised with the names
    of the failed files.

    Args:
        esperoj (object): An object containing the necessary databases, storages, and loggers.

    Raises:
        VerificationError: If the verification of one or more files fails.
    """
    logger = esperoj.loggers["Primary"]
    files = (
        esperoj.databases["Primary"]
        .get_table("Files")
        .query("$[\\Created][?@['Internet Archive'] != 'https://example.com/']")
    )
    num_shards = 28
    shard_size, extra = divmod(len(files), num_shards)
    today = datetime.datetime.now(datetime.UTC).day % num_shards

    failed_files = []

    def verify_file(file):
        """Verify the integrity of a single file.

        Args:
            file (dict): A dictionary containing the file metadata.

        Returns:
            bool: True if the file verification succeeded, False otherwise.
        """
        name = file["Name"]

        def calculate_hash_from_storage_name(storage_name):
            return calculate_hash(esperoj.storages[storage_name].get_file(name))

        def calculate_hash_from_archive():
            if file["Archive Verified"]:
                request = requests.head(file["Internet Archive"])
                if int(request.headers["content-length"]) != file["Size"]:
                    return f"""
                    HEADERS: {request.headers}
                    TEXT: {request.text}
                    """
                return file["SHA256"]
            else:
                return calculate_hash(
                    requests.get(file["Internet Archive"], stream=True, timeout=30).iter_content(
                        2**20
                    )
                )

        try:
            start_time = time.time()
            logger.info(f"Start verifying file `{name}`")
            hash_list = [file["SHA256"]]

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=min(os.cpu_count(), 4)
            ) as executor:
                futures = [
                    executor.submit(calculate_hash_from_storage_name, storage_name)
                    for storage_name in file["Storages"]
                ]
                futures.append(executor.submit(calculate_hash_from_archive))
                for future in concurrent.futures.as_completed(futures):
                    hash_list.append(future.result())
                if len(set(hash_list)) == 1:
                    logger.info(f"Verified file `{name}` in {time.time() - start_time} seconds")
                    if not file["Archive Verified"]:
                        file.update({"Archive Verified": True})
                    return True
                raise VerificationError(
                    f"Verification failed for '{name}' with hash list {hash_list}"
                )
        except VerificationError as e:
            logger.error(f"VerificationError: {e}")
            failed_files.append(name)
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            failed_files.append(name)
            return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(os.cpu_count(), 8)) as executor:
        begin = (shard_size + 1) * today if today < extra else shard_size * today
        end = begin + shard_size + (1 if today < extra else 0)
        executor.map(verify_file, files[begin:end])

    if failed_files:
        logger.error(f"Verification failed for the following files: {', '.join(failed_files)}")
        raise VerificationError("Verification failed for one or more files.")


def get_esperoj_method(esperoj):
    """Create a partial function with esperoj object.

    Args:
        esperoj (object): An object to be passed as an argument to the partial function.

    Returns:
        functools.partial: A partial function with esperoj object bound to it.
    """
    return partial(daily_verify, esperoj)


def get_click_command():
    """Create a Click command for executing the daily_verify function.

    Returns:
        click.Command: A Click command object.
    """
    import click

    @click.command()
    @click.pass_obj
    def click_command(esperoj):
        """Execute the daily_verify function with the esperoj object.

        Args:
            esperoj (object): An object passed from the parent function.
        """
        daily_verify(esperoj)

    return click_command
