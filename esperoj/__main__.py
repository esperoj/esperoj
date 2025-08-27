import os
import sys
import django
from esperoj.cli import cli_group

# Add the project directory to PYTHONPATH
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "esperoj.settings")

if __name__ == "__main__":
    django.setup()
    cli_group()
