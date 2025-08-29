from django.apps import AppConfig
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from esperoj.storages.esperoj import EsperojFileSystem


logger = logging.getLogger(__name__)


class EsperojAppConfig(AppConfig):
    """
    Application configuration for the Esperoj app.

    This config ensures that the EsperojFileSystem is initialized only after
    Django's app registry is fully loaded, preventing AppRegistryNotReady errors.
    """

    name = "esperoj"
    verbose_name = "Esperoj File Storage"
    esperoj_fs: "EsperojFileSystem | None" = None  # Declare as a class attribute

    def ready(self):
        """
        Initializes the EsperojFileSystem once the Django app registry is ready.
        """
        logger.info("EsperojAppConfig.ready() called. Configuring Esperoj File System...")
        # Import config here to avoid AppRegistryNotReady error during initial import
        # and to ensure configure_esperoj_filesystem is called at the correct time.
        from esperoj.storages import config

        self.esperoj_fs = config.configure_esperoj_filesystem()  # Assign to instance attribute
        logger.info("Esperoj File System configured.")
