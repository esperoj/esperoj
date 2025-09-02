"""
URL parsing and manipulation utilities.

This module provides pure, stateless functions for handling URLs, such as
extracting components or validating structure. These utilities are designed
to be reusable and have no dependencies on Django.
"""

from __future__ import annotations
from urllib.parse import urlparse


def get_domain_from_url(url: str) -> str:
    """
    Extracts the domain name (netloc) from a URL.

    Args:
        url: The URL string to parse.

    Returns:
        The domain name from the URL, or an empty string if parsing fails.
    """
    if not url:
        return ""
    try:
        return urlparse(url.strip()).netloc
    except (ValueError, AttributeError):
        # Handle cases where url is not a string or malformed
        return ""
