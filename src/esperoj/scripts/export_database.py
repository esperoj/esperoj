import json
from functools import partial


def export_database(esperoj, name):
    db = esperoj.databases[name]
    metadata = db.metadata
    for table in metadata["tables"]:
        table_name = table["name"]
        data = [record.to_dict() for record in db.get_table(table_name).query("$[*]")]
        with open(f"{table_name}.json", "w") as f:
            json.dump(data, f)
    with open("Metadata.json", "w") as f:
        json.dump(metadata, f)


def get_esperoj_method(esperoj):
    return partial(export_database, esperoj)


def get_click_command():
    import click

    @click.command()
    @click.argument("name", type=click.STRING, required=True)
    @click.pass_obj
    def click_command(esperoj, name):
        export_database(esperoj, name)

    return click_command
