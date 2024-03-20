"""Esperoj CLI."""

from pathlib import Path
import click
import tomllib


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
        scripts_folder = Path(__file__).parent / "scripts"
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
        mod = __import__(f"esperoj.scripts.{cmd_name}", None, None, ["get_click_command"])
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

    if not config_file:
        config_file = Path.home() / ".config" / "esperoj" / "esperoj.toml"
    config = tomllib.loads(config_file.read_text())
    esperoj = EsperojFactory.create(config)
    ctx.obj = esperoj


if __name__ == "__main__":
    cli()
