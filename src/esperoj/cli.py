"""Esperoj CLI."""


from pathlib import Path

import typer

from esperoj import create_esperoj

app = typer.Typer()
esperoj = create_esperoj()


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
