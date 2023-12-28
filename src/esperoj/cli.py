"""Esperoj CLI."""

import logging


from pathlib import Path

import typer

from esperoj import Esperoj
from esperoj.scripts import app as run

esperoj = Esperoj()
logger = logging.getLogger(__name__)
app = typer.Typer()
app.add_typer(run, name="run", context_settings={"obj": esperoj})


@app.command()
def ingest(path: Path) -> None:
    """Ingest a file.

    Args:
        path (Path): The path of the file to ingest.
    """
    esperoj.ingest(path)


@app.command()
def archive(record_id: str) -> None:
    """Archive a file in repository.

    Args:
        record_id (str): The id of the file.
    """
    logger.info(esperoj.archive(record_id))


@app.command()
def verify(record_id: str) -> None:
    """Verify a file in repository.

    Args:
        record_id (str): The id of the file.
    """
    logger.info(esperoj.verify(record_id))
