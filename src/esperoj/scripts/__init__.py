"""Module to run scripts."""
import concurrent.futures
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
def verify(ctx: Context) -> None:
    """Verifies the integrity of files by weekday.

    Args:
        ctx: The Typer context.
    """
    es = get_obj(ctx)
    files = list(es.db.table("Files").get_all(sort=["-Created"]))
    num_shards = 7
    shard_size, extra = divmod(len(files), num_shards)
    today = datetime.datetime.now(datetime.UTC).weekday()

    def verify_file(file):
        name = file.fields["Name"]
        print(f"Start to verify file `{name}`")
        start_time = time.time()
        try:
            if es.verify(file.record_id) is not True:
                raise VerifyError(f"Failed to verify {name}")
        except VerifyError as e:
            print(e)
            return False
        print(f"Verified file `{name}` in {time.time() - start_time} second")
        return True

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        begin = (shard_size + 1) * today if today < extra else shard_size * today
        end = begin + shard_size + (1 if today < extra else 0)
        executor.map(verify_file, files[begin:end])


if __name__ == "__main__":
    app()

__all__ = ["app"]
