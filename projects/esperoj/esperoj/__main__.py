"""Esperoj CLI."""

from esperoj.cli import cli


def nuitka():
    """Use to let nuitka knows what I need."""
    import esperoj.utils

    esperoj.utils.calculate_hash(b"hello")


if __name__ == "__main__":
    cli()
