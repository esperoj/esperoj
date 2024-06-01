"""Module that contains the Esperoj class, which can ingest and archive files."""

import logging

from esperoj.database.database import DatabaseFactory
from esperoj.storage.storage import StorageFactory
import tomllib
from py7zr import SevenZipFile
from os import getenv
from pathlib import Path


class Esperoj:
    """
    The Esperoj class is responsible for managing databases, storages, and loggers.

    Args:
        config (dict): The configuration dictionary for the Esperoj instance.
        databases (dict): A dictionary mapping database names to database instances.
        storages (dict): A dictionary mapping storage names to storage instances.
        loggers (dict): A dictionary mapping logger names to logger instances.
    """

    def __init__(
        self,
        config: dict,
        databases,
        storages,
        loggers,
    ):
        self.config = config
        self.databases = databases
        self.loggers = loggers
        self.storages = storages

    def __getattr__(self, name):
        """
        Dynamically import and return a method from the esperoj.scripts module.

        Args:
            name (str): The name of the method to import.

        Returns:
            callable: The imported method, or None if the import fails.
        """
        try:
            mod = __import__(f"{name}", None, None, ["get_esperoj_method"])
        except ImportError:
            return
        return mod.get_esperoj_method(self)


class EsperojFactory:
    """EsperojFactory class for creating Esperoj instances."""

    @staticmethod
    def create(config_file: str = ""):
        """
        Create and return an Esperoj instance with the specified configuration.

        Args:
            config_file (str): The configuration file path.

        Returns:
            Esperoj: The created Esperoj instance.
        """
        storages = {}
        databases = {}
        loggers = {}
        logger = logging.getLogger("Esperoj")
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        loggers["Primary"] = logger
        config_text = ""
        if not config_file:
            config_file = Path.home() / ".config" / "esperoj" / "esperoj.toml"
        if Path(config_file).suffix == ".7z":
            with SevenZipFile(str(config_file), password=getenv("ENCRYPTION_PASSPHRASE")) as zip:
                for name, bio in zip.readall().items():
                    config_text = bio.read().decode("utf-8")
        else:
            config_text = config_file.read_text()
        config = tomllib.loads(config_text)
        for storage_config in config["storages"]:
            storage = StorageFactory.create(storage_config)
            for name in [storage.config["name"]] + storage.config["aliases"]:
                storages[name] = storage
        for database_config in config["databases"]:
            database = DatabaseFactory.create(database_config)
            for name in [database.config["name"]] + database.config["aliases"]:
                databases[name] = database
        return Esperoj(config=config, databases=databases, storages=storages, loggers=loggers)
