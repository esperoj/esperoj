"""Script to archive."""

import click
import time


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


esperoj_method = daily_archive


@click.command()
@click.pass_obj
def click_command(esperoj):
    daily_archive(esperoj)
