"""Esperoj CLI."""


from pathlib import Path

import typer

from esperoj import create_esperoj
from esperoj.scripts import app as run

esperoj = create_esperoj()
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
def archive(name: str) -> None:
    """Archive a file in repository.

    Args:
        name (str): The name of the file.
    """
    print(esperoj.archive(name))
