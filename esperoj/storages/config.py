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


def configure_esperoj_filesystem() -> EsperojFileSystem:
    """
    Configures and returns an instance of EsperojFileSystem based on
    environment variables.

    This function sets up various fsspec-compatible backends and registers
    them with the EsperojFileSystem.

    Environment Variables:
        ESPEROJ_DEFAULT_STORAGE (str): The name of the default storage backend.
                                      Defaults to 'catbox'.
        ESPEROJ_CATBOX_ENABLED (str): "true" or "false" to enable/disable Catbox storage.
                                     Defaults to "true".
        CATBOX_API_URL (str): The API URL for the Catbox-like service.
                              Defaults to "https://catbox.moe/user/api.php".
        ESPEROJ_PRIMARY_STORAGES (str): Comma-separated list of storage names to be used as primary.
        ESPEROJ_BACKUP_STORAGES (str): Comma-separated list of storage names to be used as backup.
        ESPEROJ_ARCHIVE_STORAGES (str): Comma-separated list of storage names to be used as archive.

    Returns:
        EsperojFileSystem: An instantiated EsperojFileSystem with configured backends.

    Raises:
        ValueError: If configuration is missing or invalid.
    """
    configured_filesystems: dict[str, AbstractFileSystem] = {}

    # --- Catbox Configuration ---
    catbox_enabled = os.getenv("ESPEROJ_CATBOX_ENABLED", "true").lower() == "true"
    if catbox_enabled:
        catbox_api_url = os.getenv("CATBOX_API_URL", "https://catbox.moe/user/api.php")
        # Assuming CatboxFileSystem can take api_url as an argument
        catbox_fs = CatboxFileSystem(api_url=catbox_api_url)
        configured_filesystems[StorageName.CATBOX.value] = catbox_fs
        logger.info("Catbox storage backend '%s' configured with URL: %s.", StorageName.CATBOX.value, catbox_api_url)
    else:
        logger.info("Catbox storage backend is disabled via ESPEROJ_CATBOX_ENABLED.")

    # --- Internet Archive Configuration ---
    ia_enabled = os.getenv("ESPEROJ_IA_ENABLED", "false").lower() == "true"
    if ia_enabled:
        ia_access_key = os.getenv("IA_ACCESS_KEY")
        ia_secret_key = os.getenv("IA_SECRET_KEY")
        ia_collection = os.getenv("IA_COLLECTION", "test_collection")

        if ia_access_key and ia_secret_key:
            ia_fs = InternetArchiveFileSystem(
                access_key=ia_access_key, secret_key=ia_secret_key, collection=ia_collection
            )
            configured_filesystems[StorageName.INTERNET_ARCHIVE.value] = ia_fs
            logger.info("Internet Archive storage backend '%s' configured.", StorageName.INTERNET_ARCHIVE.value)
        else:
            logger.warning(
                "Internet Archive storage is enabled but IA_ACCESS_KEY or IA_SECRET_KEY are missing. Backend will not be available."
            )
    else:
        logger.info("Internet Archive storage backend is disabled via ESPEROJ_IA_ENABLED.")

    # Add other storage backends here as needed (e.g., S3, local, GCS)
    local_enabled = os.getenv("ESPEROJ_LOCAL_ENABLED", "false").lower() == "true"
    if local_enabled:
        from fsspec.implementations.local import LocalFileSystem

        local_base_path = os.getenv("ESPEROJ_LOCAL_BASE_PATH", "/tmp/esperoj_local_storage")
        os.makedirs(local_base_path, exist_ok=True)
        local_fs = LocalFileSystem(root_dir=local_base_path)
        configured_filesystems[StorageName.LOCAL_DEFAULT.value] = local_fs
        logger.info("Local storage backend '%s' configured at %s.", StorageName.LOCAL_DEFAULT.value, local_base_path)
    else:
        logger.info("Local storage backend is disabled via ESPEROJ_LOCAL_ENABLED.")

    if not configured_filesystems:
        raise ValueError("No storage backends are configured. Please enable at least one via environment variables.")

    # Determine the default storage backend
    default_storage_name_str = os.getenv("ESPEROJ_DEFAULT_STORAGE", StorageName.CATBOX.value)

    if default_storage_name_str not in configured_filesystems:
        if configured_filesystems:
            default_storage_name_str = next(iter(configured_filesystems.keys()))
            logger.warning(
                "Default storage '%s' not found or disabled. Falling back to '%s'.",
                os.getenv("ESPEROJ_DEFAULT_STORAGE"),
                default_storage_name_str,
            )
        else:
            raise ValueError("No active storage backends available, cannot set a default.")

    default_storage_name = default_storage_name_str

    # Define storage type lists from environment variables.
    # These environment variables should contain comma-separated storage backend NAMES (strings).
    configured_primary_storages = [s.strip() for s in os.getenv("ESPEROJ_PRIMARY_STORAGES", "").split(",") if s.strip()]
    configured_backup_storages = [s.strip() for s in os.getenv("ESPEROJ_BACKUP_STORAGES", "").split(",") if s.strip()]
    configured_archive_storages = [s.strip() for s in os.getenv("ESPEROJ_ARCHIVE_STORAGES", "").split(",") if s.strip()]

    # Filter out any storage names that are not actually configured.
    # This acts as validation for the environment variables.
    primary_storages_filtered = [s for s in configured_primary_storages if s in configured_filesystems]
    backup_storages_filtered = [s for s in configured_backup_storages if s in configured_filesystems]
    archive_storages_filtered = [s for s in configured_archive_storages if s in configured_filesystems]

    # Fallback: If no primary storages are explicitly defined, use the default_storage_name as primary.
    if not primary_storages_filtered and configured_filesystems:
        primary_storages_filtered.append(default_storage_name)
        logger.info("Primary storages not explicitly set, defaulting to '%s'.", default_storage_name)

    # --- Mapping from ReplicaType to Storage Backends for Writes ---
    replica_type_backend_mapping: dict[str, list[str]] = {}

    for rt in ReplicaType:
        env_var_name = f"ESPEROJ_REPLICA_{rt.name}_BACKENDS"  # e.g., ESPEROJ_REPLICA_ORIGINAL_BACKENDS
        backend_names_str = os.getenv(env_var_name, "")

        if backend_names_str:
            backend_names = [s.strip() for s in backend_names_str.split(",") if s.strip()]
            valid_backend_names = [s for s in backend_names if s in configured_filesystems]

            if valid_backend_names:
                replica_type_backend_mapping[rt.value] = valid_backend_names
                logger.info(
                    "Replica type '%s' configured to use backends: %s", rt.value, ", ".join(valid_backend_names)
                )
            else:
                logger.warning(
                    "Replica type '%s' defined with backends '%s', but none are configured or valid. This replica type will not be stored.",
                    rt.value,
                    backend_names_str,
                )
        else:
            logger.debug("No specific backends defined for replica type '%s' via %s.", rt.value, env_var_name)

    return EsperojFileSystem(
        filesystems=configured_filesystems,
        default_storage=default_storage_name,
        primary_storages=primary_storages_filtered,
        backup_storages=backup_storages_filtered,
        archive_storages=archive_storages_filtered,
        replica_type_backend_mapping=replica_type_backend_mapping,
    )


# Instantiate the EsperojFileSystem once for application-wide use
esperoj_fs = configure_esperoj_filesystem()

__all__ = ["esperoj_fs", "configure_esperoj_filesystem"]
