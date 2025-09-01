import fsspec
import logging
import logging.handlers
from django.apps import AppConfig
from django.conf import settings
import os

from esperoj.storages.catbox import CatboxFileSystem
from esperoj.storages.internet_archive import InternetArchiveFileSystem
from esperoj.storages.wayback_machine import WaybackMachineFileSystem


class CustomFormatter(logging.Formatter):
    def format(self, record):
        return super().format(record)


class EsperojConfig(AppConfig):
    name = "esperoj"
    verbose_name = "Esperoj Core Application"

    # Declare fsspec file system instances as attributes
    esperoj_fs_catbox: CatboxFileSystem | None = None
    esperoj_fs_internet_archive: InternetArchiveFileSystem | None = None
    esperoj_fs_wayback_machine: WaybackMachineFileSystem | None = None

    def ready(self):
        """
        Set up custom logging for the 'esperoj' app when Django starts
        and configure the fsspec storage backends.
        The log level is set to INFO in 'production' and DEBUG otherwise.
        """
        # --- Start of Logging Configuration ---
        logger = logging.getLogger(self.name)

        # Avoid duplicate handlers if runserver reloads
        if logger.handlers:
            logger.debug("Logger for '%s' already configured, skipping re-configuration.", self.name)
            return

        # Determine the log level based on the environment setting
        # Using settings.debug directly as per Pydantic config
        log_level = logging.DEBUG if settings.debug else logging.INFO

        logger.setLevel(log_level)  # <-- Set the logger's gatekeeper level
        logger.propagate = False

        # --- Formatter and Handler Setup ---
        log_format = "%(asctime)s - %(name)s - [%(levelname)s] - %(message)s (%(pathname)s:%(lineno)d)"
        formatter = CustomFormatter(log_format)

        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        file_handler = logging.handlers.RotatingFileHandler(
            f"{log_dir}/{self.name}.log",
            maxBytes=5 * 1024 * 1024,
            backupCount=4,
            encoding="utf-8",  # backupCount=4 for 25MB total
        )

        file_handler.setLevel(log_level)  # <-- Set the handler's gatekeeper level
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)

        logger.info(
            "Logging for '%s' app configured successfully. Log level: %s", self.name, logging.getLevelName(log_level)
        )

        # --- fsspec Storage Backend Configuration ---
        logger.info("Configuring fsspec storage backends...")

        # Catbox File System
        try:
            self.esperoj_fs_catbox = CatboxFileSystem(
                api_url=settings.catbox.api_url,
                userhash=settings.catbox.userhash,
            )
            fsspec.register_implementation(self.esperoj_fs_catbox.protocol, self.esperoj_fs_catbox)
            logger.info("CatboxFileSystem registered with protocol: %s", self.esperoj_fs_catbox.protocol)
        except ValueError as e:
            logger.error("Failed to configure CatboxFileSystem: %s", e)

        # Internet Archive File System
        try:
            # Check if access_key and secret_key are provided
            if settings.internet_archive.access_key and settings.internet_archive.secret_key:
                self.esperoj_fs_internet_archive = InternetArchiveFileSystem(
                    access_key=settings.internet_archive.access_key,
                    secret_key=settings.internet_archive.secret_key,
                )
                fsspec.register_implementation(
                    self.esperoj_fs_internet_archive.protocol, self.esperoj_fs_internet_archive
                )
                logger.info(
                    "InternetArchiveFileSystem registered with protocol: %s", self.esperoj_fs_internet_archive.protocol
                )
            else:
                logger.warning(
                    "InternetArchiveFileSystem skipped: 'access_key' or 'secret_key' is missing in settings."
                )
        except ValueError as e:
            logger.error("Failed to configure InternetArchiveFileSystem: %s", e)

        # Wayback Machine File System
        try:
            # Check if access_key and secret_key are provided
            if settings.wayback_machine.access_key and settings.wayback_machine.secret_key:
                self.esperoj_fs_wayback_machine = WaybackMachineFileSystem(
                    access_key=settings.wayback_machine.access_key,
                    secret_key=settings.wayback_machine.secret_key,
                )
                # WaybackMachineFileSystem uses "http" and "https" protocols, which are built-in.
                # We register it as a custom protocol to allow explicit usage if desired,
                # but direct 'http://' or 'https://' URLs will still use the default requests-based handler
                # unless a custom protocol name (e.g., 'wayback') is associated.
                # For SPN2, the 'write' operation implicitly handles the archival,
                # so the protocol definition here is more for consistency.
                # Check if the filesystem was successfully initialized before registering its protocols
                if self.esperoj_fs_wayback_machine:
                    for proto in self.esperoj_fs_wayback_machine.protocol:
                        fsspec.register_implementation(proto, self.esperoj_fs_wayback_machine)
                        logger.info("WaybackMachineFileSystem registered for protocol: %s", proto)
            else:
                logger.warning("WaybackMachineFileSystem skipped: 'access_key' or 'secret_key' is missing in settings.")
        except ValueError as e:
            logger.error("Failed to configure WaybackMachineFileSystem: %s", e)

        logger.info("fsspec storage backend configuration complete.")
