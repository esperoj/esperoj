"""
Date and time utility functions.

This module provides pure, stateless functions for formatting and manipulating
dates and times. These utilities are designed to be reusable across different
layers of the application and have no dependencies on Django or other external
frameworks.
"""

from __future__ import annotations
import datetime


def format_person_display_name_with_dates(
    authorized_name: str,
    birth_date: datetime.date | None,
    death_date: datetime.date | None,
) -> str:
    """
    Formats a person's name with their birth and death dates in parentheses.

    Args:
        authorized_name: The person's full name.
        birth_date: The person's date of birth.
        death_date: The person's date of death.

    Returns:
        A formatted string, e.g., "Martin Luther King, Jr. (1929–1968)".
    """

    birth_year_str = str(birth_date.year) if birth_date else "?"
    death_year_str = str(death_date.year) if death_date else "?"

    if birth_date and death_date:
        return f"{authorized_name} ({birth_year_str}–{death_year_str})"
    elif birth_date:  # Only birth date is present
        return f"{authorized_name} (b. {birth_year_str})"
    elif death_date:  # Only death date is present
        return f"{authorized_name} (?—{death_year_str})"
    return authorized_name


def format_item_display_date(
    year: int | None,
    month: int | None,
    day: int | None,
) -> str:
    """
    Formats a date from year, month, and day parts into a display string.

    Handles BCE years and partial dates gracefully.

    Args:
        year: The year (can be negative for BCE).
        month: The month (1-12).
        day: The day (1-31).

    Returns:
        A formatted date string, e.g., "January 15, 1929", "450 BCE", or "Unknown date".
    """
    if year is None:
        return "Unknown date"

    year_str = f"{abs(year)} BCE" if year < 0 else str(year)

    if month and day and year > 0:
        try:
            date_obj = datetime.date(year, month, day)
            return f"{date_obj.strftime('%B %d')}, {year_str}"
        except ValueError:
            # Fall through if the date is invalid (e.g., Feb 30)
            pass

    if month and year > 0:
        try:
            month_name = datetime.date(2000, month, 1).strftime("%B")
            return f"{month_name} {year_str}"
        except ValueError:
            # Fall through if the month is invalid
            pass

    return year_str
