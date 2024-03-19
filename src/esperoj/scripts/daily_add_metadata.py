"""Script to add metadata daily."""

from functools import partial
from pathlib import Path
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed


def process_file(esperoj, file):
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
    return partial(daily_add_metadata, esperoj)


def get_click_command():
    import click

    @click.command()
    @click.argument("number", type=click.INT, required=False, default=8)
    @click.pass_obj
    def click_command(esperoj, number):
        daily_add_metadata(esperoj, number)

    return click_command
