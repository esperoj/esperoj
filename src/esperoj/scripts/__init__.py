"""Module to run scripts."""
import datetime
import time

import typer

from esperoj import Esperoj

app = typer.Typer()
Context = typer.Context


class VerifyError(Exception):
    """Error when file integrity is compromised."""


def get_obj(ctx: Context) -> Esperoj:
    """Gets the Esperoj object from the Typer context.

    Args:
        ctx: The Typer context.

    Returns:
        The Esperoj object.
    """
    return ctx.obj


@app.command()
def archive(ctx: Context) -> None:
    """Archives all files that have not been archived yet.

    Args:
        ctx: The Typer context.
    """
    es = get_obj(ctx)
    files = es.db.table("Files").get_all({"Internet Archive": ""})
    for file in files:
        start = time.time()
        name = file.fields["Name"]
        print(f"Start to archive file `{name}`")
        es.archive(file.record_id)
        print(f"Finish file `{name}` in {time.time() - start} second")


@app.command()
def verify(ctx: Context) -> None:
    """Verifies the integrity of files by weekday.

    Args:
        ctx: The Typer context.
    """
    es = get_obj(ctx)
    files = list(es.db.table("Files").get_all(sort=["-Created"]))
    num_shards = 28
    shard_size = len(files) // num_shards
    today = datetime.datetime.now(datetime.UTC).weekday()
    for i in range(shard_size):
        file = files[today * shard_size + i]
        name = file.fields["Name"]
        print(f"Start to verify file `{name}`")
        start = time.time()
        if es.verify(file.record_id) is not True:
            raise VerifyError(f"Failed to verify {name}")
        print(f"Verified file `{name}` in {time.time() - start} second")


if __name__ == "__main__":
    app()

__all__ = ["app"]
