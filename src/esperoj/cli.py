"""Esperoj CLI."""

from pathlib import Path
import click
import tomllib

esperoj_config_file = Path.home() / ".config" / "esperoj" / "esperoj.toml"
config = tomllib.loads(esperoj_config_file.read_text())
scripts_folder = Path(__file__).parent / "scripts"


class EsperojCLI(click.Group):
    def list_commands(self, ctx):
        rv = [file.stem for file in scripts_folder.glob("*.py") if file.name != "__init__.py"]
        rv.sort()
        return rv

    def get_command(self, ctx, name):
        mod = __import__(f"esperoj.scripts.{name}", None, None, ["click_command"])
        return mod.click_command


@click.command(cls=EsperojCLI)
@click.option("--config-file", envvar="ESPEROJ_CONFIG_FILE", default=esperoj_config_file)
@click.option("--debug/--no-debug", default=False, envvar="ESPEROJ_DEBUG")
@click.pass_context
def cli(ctx, config_file, debug):
    from esperoj.esperoj import EsperojFactory

    esperoj = EsperojFactory.create(config)
    ctx.obj = esperoj


if __name__ == "__main__":
    cli()
