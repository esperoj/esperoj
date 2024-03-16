import click
import json


def export_database(esperoj, name):
    db = esperoj.databases[name]
    for table in db.metadata["tables"]:
        table_name = table["name"]
        data = [record.to_dict() for record in db.get_table(table_name).query("$[*]")]
        with open(f"{table_name}.json", "w") as f:
            json.dump(data, f)


esperoj_method = export_database


@click.command()
@click.argument("name", type=click.STRING, required=True)
@click.pass_obj
def click_command(esperoj, name):
    export_database(esperoj, name)
