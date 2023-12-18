"""Esperoj CLI."""

import os
from pathlib import Path

import typer

from esperoj.esperoj import Esperoj

app = typer.Typer()


config = {}
if os.environ.get("ESPEROJ_DATABASE") == "Airtable":
    from esperoj.airtable import Airtable

    config["db"] = Airtable()  # type: ignore
else:
    from esperoj.database import Database

    config["db"] = Database()  # type: ignore

if os.environ.get("ESPEROJ_STORAGE") == "Storj":
    from esperoj.storj import Storj

    config["storage"] = Storj()  # type: ignore
else:
    from esperoj.storage import Storage

    config["storage"] = Storage()  # type: ignore

esperoj = Esperoj(**config)


@app.command()
def ingest(path: Path) -> None:
    """Ingest a file."""
    esperoj.ingest(path)


@app.command()
def info() -> None:
    """Print info."""
    esperoj.info()
