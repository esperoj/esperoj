"""Script to add metadata daily."""

from functools import partial
import tempfile
from pathlib import Path
import json
import subprocess


def daily_add_metadata(esperoj, number: int = 8):
    logger = esperoj.loggers["Primary"]
    files = (
        esperoj.databases["Primary"]
        .get_table("Files")
        .query("$[\\Created][?@['Metadata'] = 'To be added']")
    )[:number]
    for file in files:
        name = file["Name"]
        path = Path(name)
        logger.info(f"Start to process `{name}`")
        with tempfile.NamedTemporaryFile(prefix=path.stem, suffix=path.suffix) as fp:
            for chunk in esperoj.storages[file["Storage"]].get_file(name):
                fp.write(chunk)
            output = subprocess.check_output(["exiftool", "-j", fp.name])
            metadata = json.loads(output)
            if not file.update({"Metadata": json.dumps(metadata)}):
                raise RuntimeError(f"Failed to add metadata to file `{name}`")
        logger.info(f"Finished processing file `{name}`")


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
