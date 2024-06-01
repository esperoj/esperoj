"""Esperoj CLI."""

from pathlib import Path
import click
from os import getenv
import sys

scripts_folder = Path.home() / "esperoj-scripts"
if getenv("ESPEROJ_SCRIPTS_FOLDER"):
    scripts_folder = Path(getenv("ESPEROJ_SCRIPTS_FOLDER"))
sys.path.append(str(scripts_folder))


class EsperojCLI(click.Group):
    """
    The EsperojCLI class is a Click command group that lists and loads Esperoj commands.
    """

    def list_commands(self, ctx):
        """
        List the available Esperoj commands by scanning the scripts folder.

        Args:
            ctx (click.Context): The Click context object.

        Returns:
            list: A list of command names.
        """
        rv = [file.stem for file in scripts_folder.glob("*.py") if file.name != "__init__.py"]
        rv.sort()
        return rv

    def get_command(self, ctx, cmd_name):
        """
        Load and return an Esperoj command by name.

        Args:
            ctx (click.Context): The Click context object.
            cmd_name (str): The name of the command to load.

        Returns:
            callable: The loaded command function.
        """
        mod = __import__(f"{cmd_name}", None, None, ["get_click_command"])
        return mod.get_click_command()


@click.command(cls=EsperojCLI)
@click.option("--config-file", envvar="ESPEROJ_CONFIG_FILE")
@click.option("--debug/--no-debug", default=False, envvar="ESPEROJ_DEBUG")
@click.pass_context
def cli(ctx, config_file, debug):
    """
    The main entry point for the Esperoj CLI.

    Args:
        ctx (click.Context): The Click context object.
        config_file (str): The path to the configuration file.
        debug (bool): Whether to enable debug mode.
    """
    from esperoj.esperoj import EsperojFactory

    esperoj = EsperojFactory.create(config_file)
    ctx.obj = esperoj


if __name__ == "__main__":
    cli()
