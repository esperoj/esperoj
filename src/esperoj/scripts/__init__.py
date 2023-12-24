"""Module to run scripts."""
import time

import typer

from esperoj import Esperoj

app = typer.Typer()
Context = typer.Context


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
        es.archive(name)
        print(f"Finish file `{name}` in {time.time() - start} second")


if __name__ == "__main__":
    app()

__all__ = ["app"]
