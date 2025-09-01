import logging
import logging.handlers
from django.apps import AppConfig
from django.conf import settings
import os


class CustomFormatter(logging.Formatter):
    def format(self, record):
        return super().format(record)


class EsperojConfig(AppConfig):
    name = "esperoj"

    def ready(self):
        """
        Set up custom logging for the 'esperoj' app when Django starts.
        The log level is set to INFO in 'production' and DEBUG otherwise.
        """
        # --- Start of Logging Configuration ---
        logger = logging.getLogger(self.name)

        # Avoid duplicate handlers if runserver reloads
        if logger.handlers:
            return

        # Determine the log level based on the environment setting
        if getattr(settings, "ENVIRONMENT", "development") == "production":
            log_level = logging.INFO
        else:
            log_level = logging.DEBUG

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

        # This log message will now tell you which level is active
        logger.info(
            "Logging for '%s' app configured successfully. Log level: %s", self.name, logging.getLevelName(log_level)
        )
