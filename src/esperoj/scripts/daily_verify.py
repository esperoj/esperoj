"""Script to verify daily."""

import concurrent.futures
import datetime
import time
from functools import partial
from esperoj.utils import calculate_hash
import requests


class VerificationError(Exception):
    """Raised when the verification of one or more files fails."""


def daily_verify(esperoj) -> None:
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
        name = file["Name"]
        try:
            start_time = time.time()
            logger.info(f"Start verifying file `{name}`")

            storage_hash = calculate_hash(esperoj.storages[file["Storage"]].get_file(name))
            backup_storage_hash = calculate_hash(
                esperoj.storages[f"Backup {file['Storage']}"].get_file(name)
            )
            archive_hash = calculate_hash(
                requests.get(file["Internet Archive"], stream=True, timeout=30).iter_content(2**20)
            )

            if storage_hash == backup_storage_hash == archive_hash == file["SHA256"]:
                logger.info(f"Verified file `{name}` in {time.time() - start_time} seconds")
                return True
            raise VerificationError(f"Verification failed for {name}")
        except VerificationError as e:
            logger.error(f"VerificationError: {e}")
            failed_files.append(name)
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            failed_files.append(name)
            return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        begin = (shard_size + 1) * today if today < extra else shard_size * today
        end = begin + shard_size + (1 if today < extra else 0)
        executor.map(verify_file, files[begin:end])

    if failed_files:
        logger.error(f"Verification failed for the following files: {', '.join(failed_files)}")
        raise VerificationError("Verification failed for one or more files.")


def get_esperoj_method(esperoj):
    return partial(daily_verify, esperoj)


def get_click_command():
    import click

    @click.command()
    @click.pass_obj
    def click_command(esperoj):
        daily_verify(esperoj)

    return click_command
