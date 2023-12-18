"""Esperoj CLI."""

import os
from pathlib import Path

import typer

from .esperoj import Esperoj

app = typer.Typer()


esperoj = Esperoj()
if os.environ.get("ESPEROJ_DATABASE") == "Airtable":
    from .airtable import Airtable

    esperoj.set_database(Airtable())
else:
    from .database import Database

    esperoj.set_database(Database())

if os.environ.get("ESPEROJ_STORAGE") == "Storj":
    from .storj import Storj

    esperoj.set_storage(Storj())
else:
    from .storage import Storage

    esperoj.set_storage(Storage())


@app.command()
def ingest(path: Path) -> None:
    """Ingest a file."""
    esperoj.ingest(path)


@app.command()
def info() -> None:
    """Print info."""
    esperoj.info()
