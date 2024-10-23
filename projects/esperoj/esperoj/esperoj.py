"""Module that contains the Esperoj class, which can ingest and archive files."""

import logging
import tomllib
from os import getenv
from pathlib import Path

from py7zr import SevenZipFile

from esperoj.database.database import DatabaseFactory
from esperoj.database.models import table_models
from esperoj.storage.file_host import FileHostFactory
from esperoj.storage.storage import StorageFactory
from esperoj.utils.utils import Utils


class Esperoj:
    """The Esperoj class is responsible for managing databases, storages, and loggers.

    Args:
        config (dict): The configuration dictionary for the Esperoj instance.
        databases (dict): A dictionary mapping database names to database instances.
        storages (dict): A dictionary mapping storage names to storage instances.
        loggers (dict): A dictionary mapping logger names to logger instances.
    """

    utils = Utils()

    def __init__(
        self,
        config: dict,
        databases,
        file_hosts,
        storages,
        loggers,
    ):
        self.config = config
        self.databases = databases
        self.file_hosts = file_hosts
        self.loggers = loggers
        self.storages = storages
        self.utils = Utils()

    def __getattr__(self, name):
        """Dynamically import and return a method from the esperoj.scripts module.

        Args:
            name (str): The name of the method to import.

        Returns:
            callable: The imported method, or None if the import fails.
        """
        try:
            mod = __import__(f"{name}", None, None, ["get_esperoj_method"])
        except ImportError:
            return None
        return mod.get_esperoj_method(self)


class EsperojFactory:
    """EsperojFactory class for creating Esperoj instances."""

    @staticmethod
    def create(config_file: str = ""):
        """Create and return an Esperoj instance with the specified configuration.

        Args:
            config_file (str): The configuration file path.

        Returns:
            Esperoj: The created Esperoj instance.
        """
        storages = {}
        databases = {}
        file_hosts = {}
        loggers = {}
        logger = logging.getLogger("esperoj")
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        loggers["primary"] = logger
        config_text = ""
        config_path = Path(config_file) if config_file else Path.home() / ".config" / "esperoj" / "esperoj.toml"
        if config_path.suffix == ".7z":
            with SevenZipFile(str(config_path), password=getenv("ENCRYPTION_PASSPHRASE")) as seven_zip_file:
                seven_zip_contents = seven_zip_file.readall()
                if seven_zip_contents is not None:
                    for _, bio in seven_zip_contents.items():
                        config_text = bio.read().decode("utf-8")
        else:
            config_text = config_path.read_text()
        config = tomllib.loads(config_text)
        for storage_config in config["storages"]:
            storage = StorageFactory.create(storage_config)
            for name in [storage_config["name"], *storage.config.get("aliases", [])]:
                storages[name] = storage
        for database_config in config["databases"]:
            database = DatabaseFactory.create(database_config, table_models)
            for name in [database.config["name"], *database.config.get("aliases", [])]:
                databases[name] = database
        for file_host_config in config["file_hosts"]:
            file_host = FileHostFactory.create(file_host_config)
            for name in [file_host_config["name"], *file_host.config.get("aliases", [])]:
                file_hosts[name] = file_host
        return Esperoj(config=config, databases=databases, file_hosts=file_hosts, storages=storages, loggers=loggers)
