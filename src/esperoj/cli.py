"""Esperoj CLI."""

from pathlib import Path

import typer

from esperoj.airtable import Airtable
from esperoj.esperoj import Esperoj
from esperoj.storj import Storj

app = typer.Typer()
db = Airtable()
storage = Storj()
esperoj = Esperoj(db=db, storage=storage)


@app.command()
def ingest(path: Path) -> None:
    """Ingest a file."""
    esperoj.ingest(path)


@app.command()
def info() -> None:
    """Print info."""
    esperoj.info()
