"""
This module contains custom storage backends for the esperoj application.
Individual storage backends are defined in their respective modules.
The configured EsperojFileSystem instance is managed through the `config` module.
"""

# The EsperojFileSystem instance is configured in EsperojAppConfig.ready() and accessed via django.apps.apps.get_app_config('esperoj').esperoj_fs
# No direct exports from here.

__all__ = []
