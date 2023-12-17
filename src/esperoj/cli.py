"""Esperoj CLI."""

import os
from pathlib import Path

import typer

from esperoj.esperoj import Esperoj

app = typer.Typer()

match os.environ.get("ESPEROJ_DATABASE"):
    case "Airtable":
        from esperoj.airtable import Airtable

        db: object = Airtable()
    case _:
        db = typer.Typer()

match os.environ.get("ESPEROJ_STORAGE"):
    case "Storj":
        from esperoj.storj import Storj

        storage: object = Storj()
    case _:
        storage = typer.Typer()

esperoj = Esperoj(db=db, storage=storage)


@app.command()
def ingest(path: Path) -> None:
    """Ingest a file."""
    esperoj.ingest(path)


@app.command()
def info() -> None:
    """Print info."""
    esperoj.info()
