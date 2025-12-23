import os
import django
from django.conf import settings


def setup_django():
    """Set up Django environment."""
    if not settings.configured:
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "esperoj.settings")
        django.setup()
