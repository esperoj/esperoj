"""Module to run scripts."""
import concurrent.futures
import datetime
import time

import typer

from esperoj.scripts.migrate import migrate
from esperoj.scripts.utils import Context, get_obj

app = typer.Typer()


class VerificationError(Exception):
    """Raised when the verification of one or more files fails."""


app.command()(migrate)


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
    """Verifies the integrity of files every 28 days and raises an error if any file verification fails.

    Args:
        ctx: The Typer context.

    Raises:
        VerificationError: If the verification of one or more files fails.
    """
    es = get_obj(ctx)
    files = list(es.db.table("Files").get_all(sort=["-Created"]))
    num_shards = 28
    shard_size, extra = divmod(len(files), num_shards)
    today = datetime.datetime.now(datetime.UTC).day % num_shards

    failed_files = []

    def verify_file(file):
        name = file.fields["Name"]
        try:
            start_time = time.time()
            es.logger.info(f"Start verifying file `{name}`")
            if es.verify(file.record_id) is not True:
                raise VerificationError(f"Verification failed for {name}")
            es.logger.info(f"Verified file `{name}` in {time.time() - start_time} seconds")
            return True
        except VerificationError as e:
            es.logger.error(f"VerificationError: {e}")
            failed_files.append(name)
            return False
        except Exception as e:
            es.logger.error(f"Unexpected error: {e}")
            failed_files.append(name)
            return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        begin = (shard_size + 1) * today if today < extra else shard_size * today
        end = begin + shard_size + (1 if today < extra else 0)
        executor.map(verify_file, files[begin:end])

    if failed_files:
        es.logger.error(f"Verification failed for the following files: {', '.join(failed_files)}")
        raise VerificationError("Verification failed for one or more files.")


if __name__ == "__main__":
    app()

__all__ = ["app"]
