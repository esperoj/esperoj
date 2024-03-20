from esperoj.database.database import Record
from esperoj.utils import calculate_hash
from pathlib import Path
from functools import partial


def ingest(esperoj, path: Path) -> Record:
    """Ingest a file into the Esperoj system.

    Args:
        esperoj (object): The Esperoj object representing the system.
        path (Path): The path to the file to be ingested.

    Returns:
        Record: The database record representing the ingested file.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        FileExistsError: If the file already exists in the system.
        RuntimeError: If the file type is not supported.
    """
    if not path.is_file():
        raise FileNotFoundError

    name = path.name
    size = path.stat().st_size
    f = path.open("rb")
    sha256sum = calculate_hash(f, algorithm="sha256")
    f.close()
    files = esperoj.databases["Primary"].get_table("Files")

    def add(storage_name):
        """Add the file to the specified storage and create a database record for it.

        Args:
            storage_name (str): The name of the storage where the file should be stored.

        Returns:
            Record: The database record representing the ingested file.

        Raises:
            FileExistsError: If the file already exists in the storage or database.
        """
        storage = esperoj.storages[storage_name]
        if storage.file_exists(name) or files.query('$[?Name = "{name}"]') != []:
            raise FileExistsError
        storage.upload_file(str(path), name)
        return files.create(
            {
                "Name": name,
                "Size": size,
                "SHA256": sha256sum,
                "Internet Archive": "https://example.com/",
                "Storage": storage_name,
                "Metadata": "To be added",
            }
        )

    match path.suffix:
        case ".flac" | ".mp3" | ".m4a":
            return add("Audio Storage")
        case _:
            raise RuntimeError("File type is not supported.")


def get_esperoj_method(esperoj):
    """Get the method to ingest files into the Esperoj system.

    Args:
        esperoj (object): The Esperoj object representing the system.

    Returns:
        function: A partial function that takes a file path as an argument and ingests the file.
    """
    return partial(ingest, esperoj)


def get_click_command():
    """Get the Click command to ingest a file into the Esperoj system.

    Returns:
        click.Command: The Click command to ingest a file.
    """
    import click

    @click.command()
    @click.argument(
        "file_path", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True
    )
    @click.pass_obj
    def click_command(esperoj, file_path: Path):
        """Ingest a file into the Esperoj system.

        Args:
            esperoj (object): The Esperoj object representing the system.
            file_path (Path): The path to the file to be ingested.
        """
        print(ingest(esperoj, file_path))

    return click_command
