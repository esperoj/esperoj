from esperoj.database.database import Record
from esperoj.utils import calculate_hash
from pathlib import Path
from functools import partial
from concurrent.futures import ProcessPoolExecutor, as_completed
import json
import subprocess


def ingest(esperoj, path: Path) -> list[Record]:
    """Ingest a file into the Esperoj system.

    Args:
        esperoj (object): The Esperoj object representing the system.
        path (Path): The path to be ingested.

    Returns:
        list(Record): The database records representing the ingested files.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        FileExistsError: If the file already exists in the system.
        RuntimeError: If the file type is not supported.
    """
    logger = esperoj.loggers["Primary"]

    def ingest_file(file_path: Path) -> Record:
        logger.info(f"Start to ingest file `{str(file_path)}`")

        name = file_path.name
        size = file_path.stat().st_size
        f = file_path.open("rb")
        sha256sum = calculate_hash(f, algorithm="sha256")
        f.close()
        metadata = json.loads(subprocess.check_output(["exiftool", "-j", str(file_path)]))[0]
        files = esperoj.databases["Primary"].get_table("Files")
        musics = esperoj.databases["Primary"].get_table("Musics")

        def upload(storage_names: list[str]) -> Record:
            """Upload the file to the specified storages and create a database record for it.

            Args:
                storage_names (list[str]): The names of the storages where the file should be stored.

            Returns:
                Record: The database record representing the ingested file.

            Raises:
                FileExistsError: If the file already exists in any of the storages or database.
            """
            if files.query('$[?Name = "{name}"]') != []:
                raise FileExistsError
            for storage_name in storage_names:
                storage = esperoj.storages[storage_name]
                if storage.file_exists(name):
                    raise FileExistsError
                storage.upload_file(str(file_path), name)

            return files.create(
                {
                    "Name": name,
                    "Size": size,
                    "SHA256": sha256sum,
                    "Internet Archive": "https://example.com/",
                    "Storages": json.dumps(storage_names),
                    "Metadata": json.dumps(metadata),
                }
            )

        match path.suffix:
            case ".flac" | ".mp3" | ".m4a":
                file_id = upload(["Audio Storage", "Backup Audio Storage"]).record_id
                return musics.create(
                    {"Title": metadata["Title"], "Artist": metadata["Artist"], "Files": [file_id]}
                )
            case _:
                raise RuntimeError("File type is not supported.")

    file_paths = []

    if path.is_dir():
        file_paths = [file_path for file_path in path.iterdir() if file_path.is_file()]
    else:
        if not path.is_file():
            raise FileNotFoundError
        file_paths = [path]

    with ProcessPoolExecutor(max_workers=4) as executor:
        results = []
        future_to_file_path = {
            executor.submit(ingest_file, file_path): file_path for file_path in file_paths
        }
        for future in as_completed(future_to_file_path):
            try:
                results.append(future.result())
                logger.info(f"Successful ingested file `{future_to_file_path[future]}`")
            except Exception as e:
                logger.error(str(e))
        return results


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
        "path", type=click.Path(exists=True, dir_okay=True, path_type=Path), required=True
    )
    @click.pass_obj
    def click_command(esperoj, path: Path):
        """Ingest a file or a folder into the Esperoj system.

        Args:
            esperoj (object): The Esperoj object representing the system.
            path (Path): The path to be ingested.
        """
        print(ingest(esperoj, path))

    return click_command
