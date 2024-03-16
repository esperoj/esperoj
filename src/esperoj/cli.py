"""Esperoj CLI."""

from pathlib import Path
import click
import tomllib


class EsperojCLI(click.Group):
    def list_commands(self, ctx):
        scripts_folder = Path(__file__).parent / "scripts"
        rv = [file.stem for file in scripts_folder.glob("*.py") if file.name != "__init__.py"]
        rv.sort()
        return rv

    def get_command(self, ctx, cmd_name):
        mod = __import__(f"esperoj.scripts.{cmd_name}", None, None, ["click_command"])
        return mod.click_command


@click.command(cls=EsperojCLI)
@click.option("--config-file", envvar="ESPEROJ_CONFIG_FILE")
@click.option("--debug/--no-debug", default=False, envvar="ESPEROJ_DEBUG")
@click.pass_context
def cli(ctx, config_file, debug):
    from esperoj.esperoj import EsperojFactory

    if not config_file:
        config_file = Path.home() / ".config" / "esperoj" / "esperoj.toml"
    config = tomllib.loads(config_file.read_text())
    esperoj = EsperojFactory.create(config)
    ctx.obj = esperoj


if __name__ == "__main__":
    cli()
