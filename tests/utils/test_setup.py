import os
from unittest import mock

from _pytest.stash import T

from esperoj.utils.setup import setup_django


@mock.patch.dict(os.environ, {}, clear=True)
@mock.patch("esperoj.utils.setup.settings", configured=False)
@mock.patch("django.setup")
def test_setup_django_not_configured(mock_django_setup, mock_settings):
    """
    Test that setup_django configures Django when it's not already configured.
    """
    setup_django()

    assert os.environ.get("DJANGO_SETTINGS_MODULE") == "esperoj.settings"

    mock_django_setup.assert_called_once()


@mock.patch("django.setup")
@mock.patch("os.environ.setdefault")
@mock.patch("esperoj.utils.setup.settings", configured=True)
def test_setup_django_already_configured(mock_settings_configured, mock_environ_setdefault, mock_django_setup):
    """
    Test that setup_django does nothing when Django is already configured.
    """
    setup_django()

    # Check that DJANGO_SETTINGS_MODULE was NOT set
    mock_environ_setdefault.assert_not_called()
    # Check that django.setup() was NOT called again
    mock_django_setup.assert_not_called()
