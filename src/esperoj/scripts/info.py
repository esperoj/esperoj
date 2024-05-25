from functools import partial


def info(esperoj):
    """Show info.

    Args:
        esperoj (object): An object passed from the parent function.

    Returns:
        None
    """
    files = esperoj.databases["Primary"].get_table("Files")
    records = files.query("$[*]")
    r = files.batch_update([(file.record_id, {"Archive Verified": True}) for file in records])
    print(r)


def get_esperoj_method(esperoj):
    """Create a partial function with esperoj object.

    Args:
        esperoj (object): An object to be passed as an argument to the partial function.

    Returns:
        functools.partial: A partial function with esperoj object bound to it.
    """
    return partial(info, esperoj)


def get_click_command():
    """Create a Click command for executing the info function.

    Returns:
        click.Command: A Click command object.
    """
    import click

    @click.command()
    @click.pass_obj
    def click_command(esperoj):
        """Execute the info function with the esperoj object.

        Args:
            esperoj (object): An object passed from the parent function.
        """
        info(esperoj)

    return click_command
