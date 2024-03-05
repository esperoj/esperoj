"""Utils module to be used in scripts."""
import typer

from esperoj import Esperoj

Context = typer.Context


def get_obj(ctx: Context) -> Esperoj:
    """Gets the Esperoj object from the Typer context.

    Args:
        ctx: The Typer context.

    Returns:
        The Esperoj object.
    """
    return ctx.obj
