"""
Centralized configuration for Esperoj storage backends.

This module provides a function to load and instantiate fsspec-compatible
storage backends, including CatboxFileSystem, using environment variables
for configuration.
"""

import os
import logging
from fsspec.spec import AbstractFileSystem
from esperoj.storages.esperoj import EsperojFileSystem
from esperoj.storages.catbox import CatboxFileSystem
from esperoj.storages.internet_archive import InternetArchiveFileSystem
from esperoj.constants import StorageName, ReplicaType

logger = logging.getLogger(__name__)


def _configure_catbox(fs_map: dict[str, AbstractFileSystem]) -> None:
    """Configures the Catbox filesystem and adds it to the filesystem map."""
    catbox_enabled = os.getenv("ESPEROJ_CATBOX_ENABLED", "true").lower() == "true"
    if not catbox_enabled:
        logger.info("Catbox storage backend is disabled via ESPEROJ_CATBOX_ENABLED.")
        return

    catbox_api_url = os.getenv("CATBOX_API_URL", "https://catbox.moe/user/api.php")
    catbox_userhash = os.getenv("CATBOX_USER_HASH")
    catbox_fs = CatboxFileSystem(api_url=catbox_api_url, userhash=catbox_userhash)
    fs_map[StorageName.CATBOX.value] = catbox_fs
    logger.info("Catbox storage backend '%s' configured with URL: %s.", StorageName.CATBOX.value, catbox_api_url)
    if catbox_userhash:
        logger.info("Catbox userhash configured, enabling file deletion.")
    else:
        logger.info("Catbox userhash not configured; uploads will be anonymous and cannot be deleted via API.")


def _configure_internet_archive(fs_map: dict[str, AbstractFileSystem]) -> None:
    """Configures the Internet Archive filesystem and adds it to the filesystem map."""
    ia_enabled = os.getenv("ESPEROJ_IA_ENABLED", "false").lower() == "true"
    if not ia_enabled:
        logger.info("Internet Archive storage backend is disabled via ESPEROJ_IA_ENABLED.")
        return

    ia_access_key = os.getenv("IA_ACCESS_KEY")
    ia_secret_key = os.getenv("IA_SECRET_KEY")
    ia_collection = os.getenv("IA_COLLECTION", "test_collection")

    if ia_access_key and ia_secret_key:
        ia_fs = InternetArchiveFileSystem(access_key=ia_access_key, secret_key=ia_secret_key, collection=ia_collection)
        fs_map[StorageName.INTERNET_ARCHIVE.value] = ia_fs
        logger.info("Internet Archive storage backend '%s' configured.", StorageName.INTERNET_ARCHIVE.value)
    else:
        logger.warning(
            "Internet Archive storage is enabled but IA_ACCESS_KEY or IA_SECRET_KEY are missing. Backend will not be available."
        )


def _configure_local(fs_map: dict[str, AbstractFileSystem]) -> None:
    """Configures the local filesystem and adds it to the filesystem map."""
    local_enabled = os.getenv("ESPEROJ_LOCAL_ENABLED", "false").lower() == "true"
    if not local_enabled:
        logger.info("Local storage backend is disabled via ESPEROJ_LOCAL_ENABLED.")
        return

    from fsspec.implementations.local import LocalFileSystem

    local_base_path = os.getenv("ESPEROJ_LOCAL_BASE_PATH", "/tmp/esperoj_local_storage")
    os.makedirs(local_base_path, exist_ok=True)
    local_fs = LocalFileSystem(root_dir=local_base_path)
    fs_map[StorageName.LOCAL_DEFAULT.value] = local_fs
    logger.info("Local storage backend '%s' configured at %s.", StorageName.LOCAL_DEFAULT.value, local_base_path)


def _get_default_storage(fs_map: dict[str, AbstractFileSystem]) -> str:
    """Determines the default storage backend from environment variables or a fallback."""
    if not fs_map:
        raise ValueError("No active storage backends available, cannot set a default.")

    default_storage_name_str = os.getenv("ESPEROJ_DEFAULT_STORAGE", StorageName.CATBOX.value)

    if default_storage_name_str not in fs_map:
        fallback_default = next(iter(fs_map.keys()))
        logger.warning(
            "Default storage '%s' not found or disabled. Falling back to '%s'.",
            os.getenv("ESPEROJ_DEFAULT_STORAGE"),
            fallback_default,
        )
        return fallback_default
    return default_storage_name_str


def _get_replica_type_backend_mapping(fs_map: dict[str, AbstractFileSystem]) -> dict[str, list[str]]:
    """Builds the mapping from replica types to backend names based on environment variables."""
    replica_type_backend_mapping: dict[str, list[str]] = {}
    for rt in ReplicaType:
        env_var_name = f"ESPEROJ_REPLICA_{rt.name}_BACKENDS"
        backend_names_str = os.getenv(env_var_name, "")
        if not backend_names_str:
            logger.debug("No specific backends defined for replica type '%s' via %s.", rt.value, env_var_name)
            continue

        backend_names = [s.strip() for s in backend_names_str.split(",") if s.strip()]
        valid_backend_names = [s for s in backend_names if s in fs_map]

        if valid_backend_names:
            replica_type_backend_mapping[rt.value] = valid_backend_names
            logger.info("Replica type '%s' configured to use backends: %s", rt.value, ", ".join(valid_backend_names))
        else:
            logger.warning(
                "Replica type '%s' defined with backends '%s', but none are configured or valid. This replica type will not be stored.",
                rt.value,
                backend_names_str,
            )
    return replica_type_backend_mapping


def _get_default_replica_types_for_write() -> list[str]:
    """Gets the default list of replica types for write operations from environment variables."""
    default_replica_types_for_write_str = os.getenv("ESPEROJ_DEFAULT_REPLICA_TYPES_FOR_WRITE")
    if not default_replica_types_for_write_str:
        logger.info(
            "ESPEROJ_DEFAULT_REPLICA_TYPES_FOR_WRITE not set. Will use EsperojFileSystem's internal default (ORIGINAL, ACCESS)."
        )
        return []

    parsed_replica_types = [rt.strip() for rt in default_replica_types_for_write_str.split(",") if rt.strip()]
    all_valid_replica_values = [rt.value for rt in ReplicaType]
    valid_replica_types = [rt for rt in parsed_replica_types if rt in all_valid_replica_values]

    if len(valid_replica_types) != len(parsed_replica_types):
        invalid_types = set(parsed_replica_types) - set(valid_replica_types)
        logger.warning(
            "Invalid replica types found in ESPEROJ_DEFAULT_REPLICA_TYPES_FOR_WRITE: %s. These will be ignored.",
            ", ".join(invalid_types),
        )

    if not valid_replica_types:
        logger.warning(
            "ESPEROJ_DEFAULT_REPLICA_TYPES_FOR_WRITE was set but contained no valid replica types. Will use EsperojFileSystem's internal default (ORIGINAL, ACCESS)."
        )
    else:
        logger.info("Default replica types for write operations configured: %s", valid_replica_types)

    return valid_replica_types


def configure_esperoj_filesystem() -> EsperojFileSystem:
    """
    Configures and returns an instance of EsperojFileSystem based on
    environment variables.

    This function sets up various fsspec-compatible backends and registers
    them with the EsperojFileSystem.

    Returns:
        EsperojFileSystem: An instantiated EsperojFileSystem with configured backends.

    Raises:
        ValueError: If configuration is missing or invalid.
    """
    # 1. Configure all available filesystem backends
    configured_filesystems: dict[str, AbstractFileSystem] = {}
    _configure_catbox(configured_filesystems)
    _configure_internet_archive(configured_filesystems)
    _configure_local(configured_filesystems)

    if not configured_filesystems:
        raise ValueError("No storage backends are configured. Please enable at least one via environment variables.")

    # 2. Determine the default storage backend
    default_storage_name = _get_default_storage(configured_filesystems)

    # 3. Define storage groups (primary, backup, archive) from environment variables
    primary_storages = [s.strip() for s in os.getenv("ESPEROJ_PRIMARY_STORAGES", "").split(",") if s.strip()]
    backup_storages = [s.strip() for s in os.getenv("ESPEROJ_BACKUP_STORAGES", "").split(",") if s.strip()]
    archive_storages = [s.strip() for s in os.getenv("ESPEROJ_ARCHIVE_STORAGES", "").split(",") if s.strip()]

    # 4. Filter storage groups to ensure they only contain configured backends
    primary_storages_filtered = [s for s in primary_storages if s in configured_filesystems]
    backup_storages_filtered = [s for s in backup_storages if s in configured_filesystems]
    archive_storages_filtered = [s for s in archive_storages if s in configured_filesystems]

    # 5. Fallback: If no primary storages are explicitly defined, use the default as primary.
    if not primary_storages_filtered:
        primary_storages_filtered.append(default_storage_name)
        logger.info("Primary storages not explicitly set, defaulting to '%s'.", default_storage_name)

    # 6. Configure mappings from ReplicaType to Storage Backends for writes
    replica_type_backend_mapping = _get_replica_type_backend_mapping(configured_filesystems)

    # 7. Configure default replica types for write operations
    default_replica_types_for_write = _get_default_replica_types_for_write()

    # 8. Instantiate and return the fully configured EsperojFileSystem
    return EsperojFileSystem(
        filesystems=configured_filesystems,
        default_storage=default_storage_name,
        primary_storages=primary_storages_filtered,
        backup_storages=backup_storages_filtered,
        archive_storages=archive_storages_filtered,
        replica_type_backend_mapping=replica_type_backend_mapping,
        default_replica_types_for_write=default_replica_types_for_write,
    )


# Instantiate the EsperojFileSystem once for application-wide use
esperoj_fs = configure_esperoj_filesystem()

__all__ = ["esperoj_fs", "configure_esperoj_filesystem"]
