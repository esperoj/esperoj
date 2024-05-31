"""Module query."""
from typing import Any

class Query:
    def __init__(self, filters: tuple[Any, str, Any]):
        """Initializes a Query instance.

        Args:
            filters (tuple): list of filters to use
        """
        self.filters = filters