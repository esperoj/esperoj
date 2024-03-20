"""Script to add metadata daily."""

from functools import partial
from pathlib import Path
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed


def process_file(esperoj, file):
    """Processes a single file by downloading, extracting metadata, updating database, and deleting.

    Args:
        esperoj: An EsperOJ instance.
        file (dict): A dictionary representing a file in the database.

    Raises:
        RuntimeError: If failed to update metadata for the file.
    """
    name = file["Name"]
    path = Path("/tmp", name)
    logger = esperoj.loggers["Primary"]
    logger.info(f"Start to process `{name}`")
    esperoj.storages[file["Storage"]].download_file(name, str(path))
    output = subprocess.check_output(["exiftool", "-j", str(path)])
    metadata = json.loads(output)[0]
    path.unlink()
    if not file.update({"Metadata": json.dumps(metadata)}):
        raise RuntimeError(f"Failed to add metadata to file `{name}`")
    logger.info(f"Finished processing file `{name}`")


def daily_add_metadata(esperoj, number: int = 8):
    """Script to add metadata to files daily.

    This script retrieves a list of files from the 'Files' table in the 'Primary' database
    that have the 'Metadata' field set to 'To be added' and a creation date. It then processes a specified number of these files concurrently
    using a thread pool. For each file, it:

      1. Downloads the file from EsperOJ storage.
      2. Uses `exiftool` to extract metadata in JSON format.
      3. Updates the 'Metadata' field in the database with the extracted metadata.
      4. Deletes the downloaded file.

    Args:
        esperoj: An EsperOJ instance.
        number (int, optional): The number of files to process concurrently. Defaults to 8.

    Returns:
        None
    """
    logger = esperoj.loggers["Primary"]
    files = (
        esperoj.databases["Primary"]
        .get_table("Files")
        .query("$[\\Created][?@['Metadata'] = 'To be added']")
    )[:number]
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_file, esperoj, file) for file in files]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(str(e))


def get_esperoj_method(esperoj):
    """Returns a partial function that adds metadata to files daily.

    Args:
        esperoj: An EsperOJ instance.

    Returns:
        A callable that takes an EsperOJ instance and an optional number of files to process,
        and adds metadata to the files daily.
    """
    return partial(daily_add_metadata, esperoj)


def get_click_command():
    """Returns a Click command that adds metadata to files daily.

    Returns:
        A Click command that executes the daily_add_metadata function.
    """

    import click

    @click.command()
    @click.argument("number", type=click.INT, required=False, default=8)
    @click.pass_obj
    def click_command(esperoj, number):
        daily_add_metadata(esperoj, number)

    return click_command
