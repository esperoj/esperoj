"""Esperoj CLI."""

from esperoj.cli import cli

def show_modules():
    """Show modules."""
    import esperoj.utils
    print(esperoj.utils)

if __name__ == "__main__":
    cli()
