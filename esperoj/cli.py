import click
from esperoj.models import Song  # Django models


@click.group()
def cli_group():
    """Main command group for the tool."""
    pass


@cli_group.command()
@click.argument("path")
def upload(path):
    """Upload a file or directory."""
    # (Implementation: save file metadata to DB, etc.)
    click.echo(f"Uploading {path}")


@cli_group.command()
@click.argument("name")
def archive(name):
    """Archive a stored file by name."""
    click.echo(f"Archiving {name}")


@cli_group.command()
@click.argument("title")
@click.option("--author", prompt=True)
def add_book(title, author):
    """Catalog a book."""
    click.echo(f"Adding book {title} by {author}")
    # (Implementation: create Book model, etc.)
