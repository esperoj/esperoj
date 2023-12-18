"""Esperoj CLI."""

import os
from pathlib import Path

import typer

from esperoj.esperoj import Esperoj

app = typer.Typer()


esperoj = Esperoj()
if os.environ.get("ESPEROJ_DATABASE") == "Airtable":
    from esperoj.airtable import Airtable

    esperoj.set_database(Airtable())
else:
    from esperoj.database import Database

    esperoj.set_database(Database())

if os.environ.get("ESPEROJ_STORAGE") == "Storj":
    from esperoj.storj import Storj

    esperoj.set_storage(Storj())
else:
    from esperoj.storage import Storage

    esperoj.set_storage(Storage())


@app.command()
def ingest(path: Path) -> None:
    """Ingest a file."""
    esperoj.ingest(path)


@app.command()
def info() -> None:
    """Print info."""
    esperoj.info()
