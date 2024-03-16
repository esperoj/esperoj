from esperoj.database.database import Record
from esperoj.utils import calculate_hash
import click
from pathlib import Path
from datetime import datetime


def ingest(esperoj, path: Path) -> Record:
    if not path.is_file():
        raise FileNotFoundError

    name = path.name
    size = path.stat().st_size
    f = path.open("rb")
    sha256sum = calculate_hash(f, algorithm="sha256")
    f.close()
    files = esperoj.databases["Primary"].get_table("Files")

    def add(storage_name):
        storage = esperoj.storages[storage_name]
        if storage.file_exists(name) or files.query('$[?Name = "{name}"]') != []:
            raise FileExistsError
        storage.upload_file(str(path), name)
        return files.create(
            {
                "Name": name,
                "Size": size,
                "SHA256": sha256sum,
                "Created": str(datetime.now()),
                "Internet Archive": "https://example.com/",
                "Storage": storage_name,
            }
        )

    match path.suffix:
        case ".flac" | ".mp3" | ".m4a":
            return add("Audio Storage")
        case _:
            raise RuntimeError("File type is not supported.")


esperoj_method = ingest


@click.command()
@click.argument(
    "file_path", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True
)
@click.pass_obj
def click_command(esperoj, file_path: Path):
    print(ingest(esperoj, file_path))
