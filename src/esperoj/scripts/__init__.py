"""Module to run scripts."""
import typer

from esperoj.scripts.archive import archive
from esperoj.scripts.verify import verify

app = typer.Typer()
app.command()(verify)
app.command()(archive)

__all__ = ["app"]
