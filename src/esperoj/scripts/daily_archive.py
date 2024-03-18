"""Script to archive."""

import time
from functools import partial


def daily_archive(esperoj) -> None:
    logger = esperoj.loggers["Primary"]
    files = (
        esperoj.databases["Primary"]
        .get_table("Files")
        .query('$[?"Internet Archive" = "https://example.com/"]')
    )

    def archive(file):
        url = esperoj.storages[file["Storage"]].get_link(file["Name"])
        archive_url = esperoj.save_page(url)
        file.update({"Internet Archive": archive_url})

    for file in files:
        start = time.time()
        name = file["Name"]
        logger.info(f"Start to archive file `{name}`")
        archive(file)
        logger.info(f"Finish file `{name}` in {time.time() - start} second")


def get_esperoj_method(esperoj):
    return partial(daily_archive, esperoj)


def get_click_command():
    import click

    @click.command()
    @click.pass_obj
    def click_command(esperoj):
        daily_archive(esperoj)

    return click_command
