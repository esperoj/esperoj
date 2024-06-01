"""Esperoj CLI."""

from esperoj.cli import cli


def show_modules():
    """
    This function let Nuitka see modules that need to be imported.
    """
    import esperoj.utils

    print(esperoj.utils)


if __name__ == "__main__":
    cli()
