import json
from functools import partial


def export_database(esperoj, name):
    """Export the data and metadata of a database to JSON files.

    Args:
        esperoj (object): An object containing the databases.
        name (str): The name of the database to export.

    Returns:
        None
    """
    db = esperoj.databases[name]
    metadata = db.metadata
    for table in metadata["tables"]:
        table_name = table["name"]
        data = [record.to_dict() for record in db.get_table(table_name).query()]
        with open(f"{table_name}.json", "w") as f:
            json.dump(data, f)
    with open("Metadata.json", "w") as f:
        json.dump(metadata, f)


def get_esperoj_method(esperoj):
    """Create a partial function with esperoj object.

    Args:
        esperoj (object): An object to be passed as an argument to the partial function.

    Returns:
        functools.partial: A partial function with esperoj object bound to it.
    """
    return partial(export_database, esperoj)


def get_click_command():
    """Create a Click command for executing the export_database function.

    Returns:
        click.Command: A Click command object.
    """
    import click

    @click.command()
    @click.argument("name", type=click.STRING, required=True)
    @click.pass_obj
    def click_command(esperoj, name):
        """Execute the export_database function with the esperoj object and database name.

        Args:
            esperoj (object): An object passed from the parent function.
            name (str): The name of the database to export.
        """
        export_database(esperoj, name)

    return click_command
