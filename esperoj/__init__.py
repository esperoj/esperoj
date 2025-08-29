"""
The `esperoj` package provides a flexible and scalable file storage solution
with multi-backend support, built on fsspec and Django ORM.

It exposes custom fsspec file systems for various storage backends,
and a central `EsperojFileSystem` that manages file metadata and replicas
across these backends.
"""

# Define default_app_config for Django to discover the AppConfig
default_app_config = "esperoj.apps.EsperojAppConfig"

# esperoj_fs will be configured in EsperojAppConfig.ready()
# We don't import it here to avoid AppRegistryNotReady errors.

__all__ = []
