# kronicle/deps/settings.py
from __future__ import annotations

from configparser import ConfigParser, ExtendedInterpolation
from functools import lru_cache
from json import dumps
from os import getenv
from typing import Any, ClassVar, TypeVar
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, PostgresDsn, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from kronicle.utils.dev_logs import log_d
from kronicle.utils.file_utils import check_is_file, expand_file_path
from kronicle.utils.str_utils import strip_quotes, validate_pg_identifier

# --------------------------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------------------------
KRONICLE_CONF = "KRONICLE_CONF"
KRONICLE_ENV = "KRONICLE_ENV"

_ENV_DEV = "dev"
_ENV_PROD = "prod"
_ENV_STAGE = "stage"


# --------------------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------------------
def _get_env() -> str:
    kronicle_env = getenv(KRONICLE_ENV, _ENV_PROD).lower().strip()
    mapping = {
        "dev": _ENV_DEV,
        "development": _ENV_DEV,
        "prod": _ENV_PROD,
        "production": _ENV_PROD,
        "stage": _ENV_STAGE,
    }
    if kronicle_env not in mapping:
        raise ValueError(f"Invalid KRONICLE_ENV value: '{kronicle_env}'. Must be one of {list(mapping.values())}")
    return mapping[kronicle_env]


# --------------------------------------------------------------------------------------------------
# Section models
# --------------------------------------------------------------------------------------------------

T = TypeVar("T", bound="IniSection")


class IniSection(BaseModel):
    """Base class for INI section models."""

    model_config = ConfigDict(frozen=True)

    section: ClassVar[str]  # subclasses must override

    @classmethod
    def from_parser(cls: type[T], parser: ConfigParser) -> T:
        """Create an instance from a ConfigParser."""
        if not cls.section:
            raise ValueError(f"Class '{cls.__name__}' must define a 'section' name")

        # Only include fields that exist in the parser; Pydantic fills defaults for missing ones
        data = {
            name: strip_quotes(parser.get(cls.section, name))
            for name in cls.model_fields
            if name not in ("section", "env") and parser.has_option(cls.section, name)
        }
        return cls(**data)

    def as_dict(self) -> dict[str, Any]:
        """Return a dictionary representation."""
        return self.model_dump()

    def json(self, **kwargs) -> str:
        """Return JSON string representation."""
        return dumps(self.as_dict(), **kwargs)

    def __str__(self) -> str:
        return self.json()


class AppSettings(IniSection):
    section = "app"

    version: str = Field(default="0.0.0")
    name: str = Field(default="Kronicle")
    id: UUID = Field(default_factory=lambda: UUID("ffffffff-62dd-490a-8f7e-b168c68da4a7"))
    description: str = Field(default="FastAPI-powered TimescaleDB microservice for storing time-series measurements")
    host: str = Field(default="localhost")
    port: int = Field(ge=1, le=65535, default=8080)
    openapi_url: str = Field(default="/openapi")
    env: str = Field(default_factory=_get_env)

    @property
    def tinyid(self) -> str:
        str_id = str(self.id)
        return str_id[0:8] + str_id[9:13]

    @property
    def prefix(self) -> str:
        return f"krcl_{self.tinyid}"

    @property
    def is_dev_env(self) -> bool:
        return self.env == _ENV_DEV

    @property
    def is_prod_env(self) -> bool:
        return self.env == _ENV_PROD

    @property
    def is_stage_env(self) -> bool:
        return self.env == _ENV_STAGE


class AuthSettings(IniSection):
    section = "auth"

    pwd_min_length: int = Field(ge=8, le=64, default=12)
    pwd_require_uppercase: bool = True
    pwd_require_lowercase: bool = True
    pwd_require_digits: bool = True
    pwd_require_special: bool = True
    pwd_special_chars: str = Field(default="!@%^*()_+-=[]{}:,.?")

    @field_validator("pwd_special_chars")
    def validate_special_chars(cls, v: str) -> str:
        """Ensure special characters don't contain dangerous characters."""
        forbidden = set("'\"\\$`;&|<>#\n\r\t")
        invalid_chars = [c for c in v if c in forbidden]

        if invalid_chars:
            raise ValueError(f"Special characters {v} contain forbidden character { invalid_chars} : {forbidden}")
        return v


class JWTSettings(IniSection):
    section = "jwt"

    expiration_minutes: int = Field(default=5, ge=1, le=1440)
    algorithm: str = "HS256"
    secret: SecretStr = Field(default_factory=lambda: SecretStr(f"{uuid4()}{uuid4()}"), min_length=16)

    def get_secret(self) -> str:
        """Safely get the secret value."""
        return str(self.secret.get_secret_value())

    @field_validator("algorithm")
    @classmethod
    def validate_algorithm(cls, v: str) -> str:
        allowed = {"HS256", "HS384", "HS512"}
        if v not in allowed:
            raise ValueError(f"Unsupported JWT algorithm '{v}'")
        return v


class DBSettings(IniSection):
    """Channel/metadata database settings."""

    section = "db"

    db_usr: str = Field(min_length=6)
    db_pwd: SecretStr = Field(min_length=6)
    db_name: str = Field(min_length=1)
    db_host: str = "localhost"
    db_port: int = Field(ge=1024, le=65535, default=5432)
    db_connection_url: SecretStr | None = None
    su_url: SecretStr | None = None

    @field_validator("db_usr", "db_name")
    @classmethod
    def validate_pg_identifier(cls, v: str) -> str:
        """
        Ensure the DB username and database name contain only safe characters.
        Only letters, numbers, and underscores are allowed.
        """
        return validate_pg_identifier(v)

    @property
    def usr(self) -> str:
        return self.db_usr

    @property
    def pwd(self) -> str:
        return self.db_pwd.get_secret_value()

    @property
    def host(self) -> str:
        return self.db_host

    @property
    def port(self) -> int:
        return self.db_port

    @property
    def name(self) -> str:
        return self.db_name

    @property
    def connection_url(self) -> str:
        if self.db_connection_url:
            return str(self.db_connection_url.get_secret_value())
        return f"postgresql://{self.usr}:{self.pwd}@{self.host}:{self.port}/{self.name}"

    def get_connection_url(self, usr: str | None, pwd: str | None) -> str:
        return f"postgresql://{usr}:{pwd}@{self.host}:{self.port}/{self.name}"

    @property
    def su_connection_url(self) -> str:
        if self.su_url:
            return str(self.su_url.get_secret_value())
        raise ValueError("SU Postgres URL is missing")

    @property
    def masked_connection_url(self) -> str | None:
        """Return URL safe for logging (password hidden)"""
        if not self.db_connection_url:
            return None
        url = self.connection_url
        if "@" in url and ":" in url.split("//")[1]:
            scheme, rest = url.split("://", 1)
            user_pass, host = rest.split("@", 1)
            if ":" in user_pass:
                user, _ = user_pass.split(":", 1)
                user_pass = f"{user}:******"
            return f"{scheme}://{user_pass}@{host}"
        return url

    @classmethod
    def _is_valid_pg_url(cls, url):
        # Ensure url is a SecretStr
        if not isinstance(url, SecretStr):
            url = SecretStr(url)
        # Validate format as PostgresDsn
        # simple validation using PostgresDsn
        try:
            PostgresDsn(url.get_secret_value())
        except Exception as e:
            raise ValueError(f"Invalid Postgres URL: {url}") from e
        return url

    @field_validator("db_connection_url", mode="before")
    @classmethod
    def wrap_and_validate_url(cls, url):
        if url is None:
            return url
        return cls._is_valid_pg_url(url)

    @field_validator("su_url", mode="before")
    @classmethod
    def wrap_and_validate_su_url(cls, url):
        if url is None:
            raise ValueError("SU Postgres URL is missing in config")
        return cls._is_valid_pg_url(url)

    @model_validator(mode="after")
    def validate_db_settings(self) -> "DBSettings":
        """Validate that we have either connection URL or credentials."""
        if not self.db_connection_url and not all([self.db_usr, self.db_pwd, self.db_name]):
            raise ValueError("Either db_connection_url or db_usr/db_pwd/db_name must be provided")
        return self


class RbacSettings(DBSettings):
    section = "rbac"
    pass


# --------------------------------------------------------------------------------------------------
# Root Settings
# --------------------------------------------------------------------------------------------------


class Settings(BaseSettings):
    """
    Main application configuration.

    Supports:
    - Environment variables
    - .env file
    - Nested fields using "__"
    """

    model_config = SettingsConfigDict(
        env_prefix="KRONICLE_",
        env_nested_delimiter="__",
        env_file=".env.kronicle",
        extra="forbid",
        frozen=True,
    )

    # These will be populated from the INI file
    app: AppSettings
    db: DBSettings
    auth: AuthSettings
    jwt: JWTSettings
    rbac: RbacSettings
    max_retries: int = Field(default=10, ge=0)

    @property
    def api_version(self) -> str:
        return "v1"

    @classmethod
    def from_parser(cls, parser: ConfigParser) -> Settings:
        """Create settings from a ConfigParser, with environment variable fallback."""
        return cls(
            app=AppSettings.from_parser(parser),
            auth=AuthSettings.from_parser(parser),
            jwt=JWTSettings.from_parser(parser),
            db=DBSettings.from_parser(parser),
            rbac=RbacSettings.from_parser(parser),
        )

    @classmethod
    def load_ini_file(cls, ini_path: str) -> Settings:
        """Load and parse INI file into a flat dictionary."""
        from configparser import ConfigParser

        log_d("ini.load", "Reading conf file", ini_path)
        path = expand_file_path(ini_path)
        check_is_file(path, f"Configuration file not found: '{path}'")

        parser = ConfigParser(interpolation=ExtendedInterpolation())
        parser.read(path)

        return cls.from_parser(parser)

    def safe_dump(self) -> dict[str, Any]:
        return self.model_dump()

    def as_dict(self) -> dict[str, Any]:
        return self.safe_dump()

    def __str__(self) -> str:
        return self.model_dump_json(indent=2)

    def model_dump_json(self, *, indent: int | None = 2, **kwargs) -> str:
        """
        Same as BaseModel.model_dump_json, but default pretty-printing with indent=2.
        All other parameters are passed through (include, exclude, by_alias, etc.).
        """
        return super().model_dump_json(indent=indent, **kwargs)

    def json(self, *, indent: int = 2, **kwargs) -> str:
        return self.model_dump_json(indent=indent, **kwargs)


@lru_cache
def get_settings(ini_path: str = "../.conf/config.ini") -> Settings:
    """Load settings from INI file and environment variables."""
    here = "ini.load"

    # Get config file path from environment variable or use default
    conf_file = getenv(KRONICLE_CONF, ini_path)
    log_d(here, "Conf file path set to", conf_file)

    # Expand and validate the file path
    ini_file = expand_file_path(conf_file)
    check_is_file(ini_file, f"Configuration file not found: '{ini_file}'")

    # Load from INI file
    return Settings.load_ini_file(ini_path)


# --------------------------------------------------------------------------------------------------
# Example usage / debug
# --------------------------------------------------------------------------------------------------
if __name__ == "__main__":  # pragma: no cover
    here = "conf"
    conf = get_settings("./.conf/config.ini")
    log_d(here, "App name", conf.app.name)
    log_d(here, "JWT expiration (minutes)", conf.jwt.expiration_minutes)
    log_d(here, "DB URL:", conf.db.connection_url)
    log_d(here, "Full config as dict\n", conf.as_dict())
    log_d(here, "Full config as JSON:\n", conf.json(indent=2))

    db_url = conf.db.connection_url
    log_d(here, "DB connection url:", db_url)
