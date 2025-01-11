"""Storage module."""

from esperoj.config import get_config
from esperoj.storage.file_host import FileHost
from esperoj.storage.storage import Storage

file_hosts = {}
storages = {}


def create_storage(config) -> Storage:
    """Create a storage instance based on the provided configuration.

    Args:
        config (dict): The configuration dictionary for the storage.

    Returns:
        storage: The storage instance corresponding to the specified type.

    Raises:
        ValueError: If the storage type in the configuration is unknown.
    """
    storage_type = config["type"]
    match storage_type:
        case "s3":
            from esperoj.storage.s3 import S3Storage

            return S3Storage(config)
    raise ValueError(f"Unknown storage type: {storage_type}")


def get_storage(name):
    if not (storage := storages.get(name)):
        for storage_config in get_config()["storages"]:
            names = [storage_config["name"], *storage_config.get("aliases", [])]
            if name in names:
                storage = create_storage(storage_config)
                for _name in names:
                    storages[_name] = storage
    return storage


def get_all_storages():
    for storage_config in get_config()["storages"]:
        get_storage(storage_config["name"])
    return storages


def create_file_host(config) -> FileHost:
    """Create a FileHost instance based on the provided configuration.

    Args:
        config (dict): The configuration dictionary for the storage.

    Returns:
        file_host: The FileHost instance corresponding to the specified type.

    Raises:
        ValueError: If the file_host type in the configuration is unknown.
    """
    file_host_type = config["type"]
    name = config["name"]
    match file_host_type:
        case "catbox":
            from esperoj.storage.catbox import Catbox

            return Catbox(name, config)

        case "internet_archive":
            from esperoj.storage.internet_archive import InternetArchive

            return InternetArchive(name, config)

        case "local_file_host":
            from esperoj.storage.local_file_host import LocalFileHost

            return LocalFileHost(name, config)
    raise ValueError(f"Unknown file host type: {file_host_type}")


def get_file_host(name):
    if not (file_host := file_hosts.get(name)):
        for file_host_config in get_config()["file_hosts"]:
            names = [file_host_config["name"], *file_host_config.get("aliases", [])]
            if name in names:
                file_host = create_file_host(file_host_config)
                for _name in names:
                    file_hosts[_name] = file_host
    return file_host


def get_all_file_hosts():
    for file_host_config in get_config()["file_hosts"]:
        get_file_host(file_host_config["name"])
    return file_hosts
