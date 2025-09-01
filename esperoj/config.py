"""
Pydantic-based settings management for the esperoj project.

This module centralizes application configuration using Pydantic's BaseSettings,
providing a single source of truth that is type-safe, validated, and easy to manage.
This approach is considered best practice for modern Django applications.

Key Benefits:
- Type Hinting: Settings are strongly typed, preventing common configuration errors.
- Validation: Pydantic automatically validates settings against their defined types
  (e.g., ensuring a database URL is correctly formatted).
- Environment Variable Parsing: Seamlessly reads from .env files and the environment,
  making it easy to manage different configurations (dev, staging, prod).
- Nested Configuration: Supports structured settings (e.g., database or cache settings)
  which keeps your configuration organized.
- Centralization: All environment-dependent settings are defined in one place.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv

# Explicitly load environment variables from .env file.
# This ensures os.environ is populated BEFORE AppSettings is instantiated,
# resolving the "Argument missing for parameter 'secret_key'" error.
load_dotenv()


# --- Nested Settings Classes ---
# It's best practice to group related settings into their own classes.
# These classes will be instantiated and populated by the main AppSettings class.


class DatabaseSettings(BaseSettings):
    """
    Manages all database connection settings.

    The fields in this class will be populated from environment variables
    that follow the pattern: `<prefix>_<parent_field_name>__<field_name>`.
    For example, `url` will be populated by `ESPEROJ_DB__URL`.
    """

    # The URL for the database connection. This will be parsed by dj-database-url
    # in Django's settings.py, allowing for different database types (e.g., SQLite, Postgres).
    url: str = "postgresql://user:pass@localhost:5432/esperoj"


class CatboxSettings(BaseSettings):
    """
    Manages settings for the Catbox storage backend.
    Populated from environment variables like `ESPEROJ_CATBOX__API_URL`.
    """

    api_url: str = Field(default="https://catbox.moe/user/api.php", description="Catbox API endpoint URL.")
    userhash: str | None = Field(default=None, description="User hash for Catbox, required for deletions.")


class InternetArchiveSettings(BaseSettings):
    """
    Manages settings for the Internet Archive storage backend.
    Populated from environment variables like `ESPEROJ_IA__ACCESS_KEY`.
    """

    access_key: str | None = Field(default=None, description="Internet Archive API access key.")
    secret_key: str | None = Field(default=None, description="Internet Archive API secret key.")


class WaybackMachineSettings(BaseSettings):
    """
    Manages settings for the Wayback Machine storage backend.
    Populated from environment variables like `ESPEROJ_WM__ACCESS_KEY`.
    """

    access_key: str | None = Field(default=None, description="Wayback Machine S3-style access key.")
    secret_key: str | None = Field(default=None, description="Wayback Machine S3-style secret key.")


# --- Main Application Settings ---
# This is the primary class that brings all the settings together.


class AppSettings(BaseSettings):
    """
    The main settings class for the application.

    This class reads from the environment and .env files, validates the data,
    and provides a single, typed object for accessing all configuration variables.
    """

    # Core Django Settings. These are top-level settings.
    # Maps to: ESPEROJ_DEBUG
    debug: bool = Field(default=False)
    # Maps to: ESPEROJ_SECRET_KEY
    # The `...` as the default value means this field is REQUIRED. If the
    # environment variable is not set, Pydantic will raise a validation error.
    secret_key: str = Field(default="django-insecure-a%b3&0=cehtz6qsagy-vpu=1jc5$!_&lvfc76d*jn_7f3nv=9v", min_length=32)

    # Nested Settings Groups
    # Use `Field(default_factory=lambda: ClassName())` to properly instantiate
    # nested BaseSettings models. Pydantic-settings will then populate them from
    # environment variables using the `env_nested_delimiter` convention.
    db: DatabaseSettings = Field(default_factory=lambda: DatabaseSettings())
    catbox: CatboxSettings = Field(default_factory=lambda: CatboxSettings())
    internet_archive: InternetArchiveSettings = Field(default_factory=lambda: InternetArchiveSettings())
    wayback_machine: WaybackMachineSettings = Field(default_factory=lambda: WaybackMachineSettings())

    model_config = SettingsConfigDict(
        env_prefix="ESPEROJ_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )


# --- Singleton Instance ---
# Create a single, importable instance of the settings.
# Your entire application will import this `settings` object.
settings = AppSettings()
