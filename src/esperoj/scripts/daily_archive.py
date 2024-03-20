"""Script to archive."""

import time
from functools import partial


def daily_archive(esperoj) -> None:
    """Script to archive files daily.

    This script retrieves a list of files from the 'Files' table in the 'Primary' database
    that have the URL "https://example.com/" set for the "Internet Archive" field. It then
    archives each file using the `esperoj.save_page` function and updates the
    "Internet Archive" field with the archived URL.

    Args:
        esperoj: An EsperOJ instance.

    Returns:
        None
    """

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
    """Returns a partial function that archives files daily.

    Args:
        esperoj: An EsperOJ instance.

    Returns:
        A callable that takes an EsperOJ instance and archives files daily.
    """
    return partial(daily_archive, esperoj)


def get_click_command():
    """Returns a Click command that archives files daily.

    Returns:
        A Click command that executes the daily_archive function.
    """
    import click

    @click.command()
    @click.pass_obj
    def click_command(esperoj):
        daily_archive(esperoj)

    return click_command
