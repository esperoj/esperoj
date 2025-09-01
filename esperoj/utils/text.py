"""
Text and string manipulation utilities.

This module provides pure, stateless functions for common text-processing tasks,
such as formatting, cleaning, and transformation. These utilities are designed
to be reusable across different layers of the application and have no dependencies
on Django or other external frameworks.
"""

from __future__ import annotations


def format_display_size(size: int | float) -> str:
    """
    Converts a size in bytes to a human-readable string with appropriate units.

    Args:
        size: The size in bytes.

    Returns:
        A formatted string with the size and unit (e.g., "1.2 MB").
    """
    if size < 0:
        return "0 B"
    current_size = float(size)
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if current_size < 1024.0:
            if unit == "B":
                return f"{int(current_size)} {unit}"
            return f"{current_size:.1f} {unit}"
        current_size /= 1024.0
    return f"{current_size:.1f} PB"


def generate_sort_name(authorized_name: str) -> str:
    """
    Generates a sortable name from a full name in direct order.

    Follows a simple "Last Name, First Name" or "Last Part, First Parts"
    convention for sorting and indexing.

    Args:
        authorized_name: The full name in direct order (e.g., "Martin Luther King, Jr.").

    Returns:
        The name in inverted order for sorting (e.g., "King, Jr., Martin Luther").
    """
    if not authorized_name:
        return ""

    name_parts = authorized_name.strip().split()
    if len(name_parts) > 1:
        last_part = name_parts[-1]
        first_parts = " ".join(name_parts[:-1])
        return f"{last_part}, {first_parts}"
    else:
        return authorized_name


def format_duration(seconds: int | None) -> str:
    """
    Formats a duration in seconds into a human-readable string (e.g., '3:45').

    Args:
        seconds: The duration in seconds.

    Returns:
        A formatted string in "M:SS" format, or an empty string if input is None.
    """
    if seconds is None or seconds < 0:
        return ""

    minutes = seconds // 60
    remaining_seconds = seconds % 60
    return f"{minutes}:{remaining_seconds:02d}"


def format_isbn(isbn: str) -> str:
    """
    Formats a raw ISBN string (10 or 13 digits) with hyphens for display.

    Args:
        isbn: The raw 10- or 13-digit ISBN string.

    Returns:
        A formatted ISBN string with hyphens, or the original string if the
        length is not 10 or 13.
    """
    if len(isbn) == 13:
        # Format ISBN-13: 978-0-123-45678-9
        return f"{isbn[:3]}-{isbn[3]}-{isbn[4:7]}-{isbn[7:12]}-{isbn[12]}"
    elif len(isbn) == 10:
        # Format ISBN-10: 0-123-45678-9
        return f"{isbn[0]}-{isbn[1:4]}-{isbn[4:9]}-{isbn[9]}"
    return isbn
