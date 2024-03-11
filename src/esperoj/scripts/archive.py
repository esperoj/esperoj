"""Script to archive."""
import time

from esperoj.scripts.utils import Context, get_obj


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


__all__ = ["archive"]
