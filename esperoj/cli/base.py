import argparse
from typing import Any, Dict


class Command:
    """Base class for CLI commands."""

    name: str
    help: str

    def add_arguments(self, parser: argparse.ArgumentParser):
        """Add arguments to the command's parser."""
        pass

    def handle(self, *args: Any, **options: Any):
        """Execute the command logic."""
        raise NotImplementedError("Subclasses must implement the handle method.")
