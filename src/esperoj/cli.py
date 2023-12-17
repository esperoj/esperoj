"""Esperoj CLI."""

from pathlib import Path

import typer

from .airtable import Airtable
from .esperoj import Esperoj
from .storj import Storj

app = typer.Typer()
db = Airtable()
storage = Storj()
esperoj = Esperoj(db=db, storage=storage)


@app.command()
def ingest(path: Path) -> None:
    esperoj.ingest(path)


@app.command()
def info() -> None:
    """
    print info
    """
    esperoj.info()
