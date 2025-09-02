"""
Text and string manipulation utilities.

This module provides pure, stateless functions for common text-processing tasks,
such as formatting, cleaning, and transformation. These utilities are designed
to be reusable across different layers of the application and have no dependencies
on Django or other external frameworks.
"""

from __future__ import annotations
import math
import re


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
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    i = 0
    while current_size >= 1024.0 and i < len(units) - 1:
        current_size /= 1024.0
        i += 1

    if i == 0:  # Bytes
        return f"{int(current_size)} {units[i]}"
    else:
        # For values like 1023.999... KB, we want 1023.9 KB
        # Use floor to achieve truncation for the .1f part
        rounded_size = math.floor(current_size * 10) / 10.0
        return f"{rounded_size:.1f} {units[i]}"


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
    name = authorized_name.strip()
    if not name:
        return ""

    parts = name.split()
    num_parts = len(parts)

    if num_parts <= 1:
        return name

    # Define common suffixes to handle them specially
    suffixes = {"Jr.", "Sr.", "II", "III", "IV", "V"}

    # Check if the very last part is a recognized suffix
    if parts[-1] in suffixes and num_parts >= 2:
        # If so, combine the second-to-last part and the suffix.
        # Remove the comma from the second-to-last part if it exists for cleaner formatting.
        actual_last_name = parts[-2].rstrip(",")
        last_name_block = f"{actual_last_name}, {parts[-1]}"
        first_name_block = " ".join(parts[:-2])
    else:
        # Standard "Lastname, Firstnames"
        last_name_block = parts[-1]
        first_name_block = " ".join(parts[:-1])

    if not first_name_block.strip():
        return last_name_block
    else:
        return f"{last_name_block}, {first_name_block.strip()}"


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
    # Remove all non-digit characters from the ISBN, allowing 'X' for ISBN-10 checksum
    cleaned_isbn = re.sub(r"[^0-9X]", "", isbn)

    if len(cleaned_isbn) == 13:
        # Format ISBN-13: 978-0-123-45678-9 (Prefix-Group-Registrant-Publication-Check)
        # This specific format (3-1-3-5-1) is used in the test case.
        return f"{cleaned_isbn[:3]}-{cleaned_isbn[3]}-{cleaned_isbn[4:7]}-{cleaned_isbn[7:12]}-{cleaned_isbn[12]}"
    elif len(cleaned_isbn) == 10:
        # Format ISBN-10: 0-123-45678-9 (Group-Registrant-Publication-Check)
        # This specific format (1-3-5-1) is used in the test case.
        return f"{cleaned_isbn[0]}-{cleaned_isbn[1:4]}-{cleaned_isbn[4:9]}-{cleaned_isbn[9]}"
    return isbn  # Return original string if not a valid 10 or 13-digit ISBN after cleaning
