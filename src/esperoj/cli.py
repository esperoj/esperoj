"""Esperoj CLI."""

import os
from pathlib import Path

import typer

from esperoj.esperoj import Esperoj

app = typer.Typer()


config = {}
if os.environ.get("ESPEROJ_DATABASE") == "Airtable":
    from esperoj.database.airtable import Airtable

    config["db"] = Airtable("Airtable")
else:
    from esperoj.database.memory import MemoryDatabase

    config["db"] = MemoryDatabase("Memory Database")

if os.environ.get("ESPEROJ_STORAGE") == "Storj":
    from esperoj.storage.s3 import S3Storage

    config["storage"] = S3Storage(
        name="Storj",
        config={
            "client_config": {
                "aws_access_key_id": os.getenv("STORJ_ACCESS_KEY_ID"),
                "aws_secret_access_key": os.getenv("STORJ_SECRET_ACCESS_KEY"),
                "endpoint_url": os.getenv("STORJ_ENDPOINT_URL"),
            },
            "bucket_name": "esperoj",
        },
    )
else:
    from esperoj.storage.local import LocalStorage

    config["storage"] = LocalStorage("Local Storage", "/tmp/esperoj")

esperoj = Esperoj(**config)


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
