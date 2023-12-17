"""Test Esperoj."""

import esperoj


def test_import() -> None:
    """Test that the package can be imported."""
    assert isinstance(esperoj.__name__, str)
