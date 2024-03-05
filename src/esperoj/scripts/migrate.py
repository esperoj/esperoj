"""Script to migrate one database to another."""
from esperoj.scripts.utils import Context, get_obj
from esperoj.utils import get_db


def migrate(ctx: Context, source: str, target: str) -> None:
    """Migrate from one database to another.

    Args:
        ctx: The Typer context.
        source (str): Name of the source database.
        target (str): Name of the target database.

    """
    es = get_obj(ctx)
    src_db = get_db(source)
    dst_db = get_db(target)

    es.logger.info("Start migrating")

    src_files_records = list(src_db.table("Files").get_all())
    dst_files_records = dst_db.table("Files").create_many(
        record.fields for record in src_files_records
    )
    files_ids_mapping = dict(
        zip(
            (r.record_id for r in src_files_records),
            (r.record_id for r in dst_files_records),
            strict=False,
        )
    )
    src_musics = src_db.table("Musics")
    dst_db.table("Musics").create_many(
        {**music.fields, "Files": [files_ids_mapping[music.fields["Files"][0]]]}
        for music in src_musics.get_all()
    )

    src_db.close()
    dst_db.close()


run = migrate
