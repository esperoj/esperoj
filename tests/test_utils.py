"""
Unit tests for the utility functions in esperoj/utils/.
"""

import datetime
from decimal import Decimal

import pytest

from esperoj.utils.checksums import calculate_checksum
from esperoj.utils.dates import format_person_display_name_with_dates, format_item_display_date
from esperoj.utils.text import format_display_size, generate_sort_name, format_duration, format_isbn
from esperoj.utils.urls import get_domain_from_url


# --- Test esperoj.utils.checksums ---


@pytest.mark.parametrize(
    "content, algorithm, expected_checksum",
    [
        (b"hello world", "md5", "5eb63bbbe01eeed093cb22bb8f5acdc3"),
        (b"hello world", "sha1", "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed"),
        (b"hello world", "sha256", "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"),
        (b"", "md5", "d41d8cd98f00b204e9800998ecf8427e"),
        (b"a" * 10000, "sha256", "27dd1f61b867b6a0f6e9d8a41c43231de52107e53ae424de8f847b821db4b711"),
    ],
)
def test_calculate_checksum_valid(content: bytes, algorithm: str, expected_checksum: str):
    """
    Test calculate_checksum with valid inputs for different algorithms and content.
    """
    from io import BytesIO

    file_obj = BytesIO(content)
    file_obj.seek(0)  # Reset file pointer to the beginning
    assert calculate_checksum(file_obj, algorithm) == expected_checksum


def test_calculate_checksum_unsupported_algorithm():
    """
    Test calculate_checksum with an unsupported hashing algorithm.
    """
    from io import BytesIO

    file_obj = BytesIO(b"data")
    with pytest.raises(ValueError, match="Unsupported hashing algorithm: invalid"):
        calculate_checksum(file_obj, "invalid")


# --- Test esperoj.utils.dates ---


@pytest.mark.parametrize(
    "name, birth, death, expected",
    [
        ("John Doe", datetime.date(1980, 1, 1), datetime.date(2020, 12, 31), "John Doe (1980–2020)"),
        ("Jane Smith", datetime.date(1990, 5, 10), None, "Jane Smith (b. 1990)"),
        ("Unknown Person", None, None, "Unknown Person"),
        ("Artist Name", None, datetime.date(1950, 6, 15), "Artist Name (?—1950)"),
        ("Only Birth Year", datetime.date(1975, 1, 1), None, "Only Birth Year (b. 1975)"),
    ],
)
def test_format_person_display_name_with_dates(
    name: str, birth: datetime.date | None, death: datetime.date | None, expected: str
):
    """
    Test format_person_display_name_with_dates with various date combinations.
    """
    assert format_person_display_name_with_dates(name, birth, death) == expected


@pytest.mark.parametrize(
    "year, month, day, expected",
    [
        (2023, 1, 15, "January 15, 2023"),
        (2023, 7, None, "July 2023"),
        (2023, None, None, "2023"),
        (None, None, None, "Unknown date"),
        (-450, None, None, "450 BCE"),  # BCE year
        (1999, 12, 31, "December 31, 1999"),
        (2024, 2, 30, "February 2024"),  # Invalid day, should fall back to month-year
        (2024, 13, 1, "2024"),  # Invalid month, should fall back to year
        (2024, 0, 1, "2024"),  # Invalid month (0), should fall back to year
        (0, 1, 1, "0"),  # Year 0, treated as a year string
        (-1, 1, 1, "1 BCE"),  # Year -1, treated as BCE
    ],
)
def test_format_item_display_date(year: int | None, month: int | None, day: int | None, expected: str):
    """
    Test format_item_display_date with various date components, including invalid dates.
    """
    assert format_item_display_date(year, month, day) == expected


# --- Test esperoj.utils.text ---


@pytest.mark.parametrize(
    "size, expected",
    [
        (0, "0 B"),
        (10, "10 B"),
        (1023, "1023 B"),
        (1024, "1.0 KB"),
        (1536, "1.5 KB"),
        (1024**2 - 1, "1023.9 KB"),
        (1024**2, "1.0 MB"),
        (1024**3, "1.0 GB"),
        (1024**4, "1.0 TB"),
        (1024**5, "1.0 PB"),
        (1024**6, "1024.0 PB"),  # Exceeds PB to show it continues
        (Decimal(123456789), "117.7 MB"),
        (-100, "0 B"),  # Negative size
    ],
)
def test_format_display_size(size: int | float, expected: str):
    """
    Test format_display_size for various byte inputs.
    """
    assert format_display_size(size) == expected


@pytest.mark.parametrize(
    "authorized_name, expected_sort_name",
    [
        ("John Doe", "Doe, John"),
        ("Jane A. Smith", "Smith, Jane A."),
        ("Dr. Martin Luther King, Jr.", "King, Jr., Dr. Martin Luther"),
        ("SingleName", "SingleName"),
        ("  Leading and Trailing Spaces  ", "Spaces, Leading and Trailing"),
        ("", ""),
        (" ", ""),
        ("Another Name", "Name, Another"),
        ("John Jr.", "John, Jr."),
        (
            "Dr. John Doe Jr.",
            "Doe, Jr., Dr. John",
        ),  # New test to cover non-empty first_name_block after suffix processing
    ],
)
def test_generate_sort_name(authorized_name: str, expected_sort_name: str):
    """
    Test generate_sort_name for various name formats.
    """
    assert generate_sort_name(authorized_name) == expected_sort_name


@pytest.mark.parametrize(
    "seconds, expected_duration",
    [
        (0, "0:00"),
        (59, "0:59"),
        (60, "1:00"),
        (90, "1:30"),
        (3600, "60:00"),
        (3661, "61:01"),
        (None, ""),
        (-10, ""),  # Negative seconds
    ],
)
def test_format_duration(seconds: int | None, expected_duration: str):
    """
    Test format_duration for various second inputs.
    """
    assert format_duration(seconds) == expected_duration


@pytest.mark.parametrize(
    "isbn, expected_formatted_isbn",
    [
        ("9780123456789", "978-0-123-45678-9"),  # Valid ISBN-13
        ("0123456789", "0-123-45678-9"),  # Valid ISBN-10
        ("978-0-12-345678-9", "978-0-123-45678-9"),  # Already formatted (cleaned by model)
        ("0-1234-567-89", "0-123-45678-9"),  # Already formatted (cleaned by model)
        ("12345", "12345"),  # Invalid length
        ("", ""),  # Empty string
        ("invalid", "invalid"),  # Non-numeric
    ],
)
def test_format_isbn(isbn: str, expected_formatted_isbn: str):
    """
    Test format_isbn for valid and invalid ISBN inputs.
    """
    assert format_isbn(isbn) == expected_formatted_isbn


# --- Test esperoj.utils.urls ---


@pytest.mark.parametrize(
    "url, expected_domain",
    [
        ("https://www.example.com/path/to/page?query=1", "www.example.com"),
        ("http://example.org:8080/another/path", "example.org:8080"),
        ("ftp://ftp.test.net/file.txt", "ftp.test.net"),
        ("http://localhost:8000/", "localhost:8000"),
        ("  https://whitespace.com  ", "whitespace.com"),  # Whitespace stripped by urlparse
        ("", ""),
        ("invalid-url-string", ""),
        (None, ""),  # Test with None input
        (123, ""),  # Test with an invalid type to trigger AttributeError
    ],
)
def test_get_domain_from_url(url: str | None, expected_domain: str):
    """
    Test get_domain_from_url for various URL inputs.
    """
    assert get_domain_from_url(url or "") == expected_domain
