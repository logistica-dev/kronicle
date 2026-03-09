# kronicle/deps/settings.py
from __future__ import annotations

from configparser import ConfigParser
from json import dumps
from typing import Any, ClassVar, TypeVar
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from kronicle.deps.env_var import ConnectionSettings, KronicleEnvConf
from kronicle.utils.file_utils import is_file, load_ini_file
from kronicle.utils.str_utils import strip_quotes

# --------------------------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------------------------
DEFAULT_CONF_FILE_PATH = "./conf/default-conf.ini"
ALT_CONF_FILE_PATH = "../conf/default-conf.ini"

T = TypeVar("T", bound="IniSection")

# --------------------------------------------------------------------------------------------------
# Section models
# --------------------------------------------------------------------------------------------------


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
    openapi_url: str = Field(default="/openapi")

    @property
    def tinyid(self) -> str:
        str_id = str(self.id)
        return str_id[0:8] + str_id[9:13]

    @property
    def prefix(self) -> str:
        return f"krcl_{self.tinyid}"


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
            raise ValueError(f"Special characters {v} contain forbidden character {invalid_chars} : {forbidden}")
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


class DBSettings:
    """
    Channel/metadata database settings.
    These are extracted from environment variables.
    """

    def __init__(self, conf: KronicleEnvConf) -> None:
        self.env = conf

        self._host: str = conf.db.host
        self._port: int = conf.db.port
        self._name: str = conf.db.name  # already gone through normalize_pg_identifier

        self._chan_usr: str = conf.chan_creds.username  # already gone through normalize_pg_identifier
        self._chan_pwd: SecretStr = SecretStr(conf.chan_creds.password)

        self._rbac_usr: str = conf.rbac_creds.username  # already gone through normalize_pg_identifier
        self._rbac_pwd: SecretStr = SecretStr(conf.rbac_creds.password)

    def get_connection_url(self, usr: str, pwd: str) -> str:
        return f"postgresql://{usr}:{pwd}@{self._host}:{self._port}/{self._name}"

    @property
    def channel_connection_url(self) -> str:
        return self.get_connection_url(usr=self._chan_usr, pwd=self._chan_pwd.get_secret_value())

    @property
    def rbac_connection_url(self) -> str:
        return self.get_connection_url(usr=self._rbac_usr, pwd=self._rbac_pwd.get_secret_value())

    @property
    def masked_connection_url(self) -> str | None:
        """Return URL safe for logging (password hidden)"""
        url = self.channel_connection_url
        if "@" in url and ":" in url.split("//")[1]:
            scheme, rest = url.split("://", 1)
            user_pass, host = rest.split("@", 1)
            if ":" in user_pass:
                user, _ = user_pass.split(":", 1)
                user_pass = f"{user}:******"
            return f"{scheme}://{user_pass}@{host}"
        return url

    # @classmethod
    # def _is_valid_pg_url(cls, url):
    #     # Ensure url is a SecretStr
    #     if not isinstance(url, SecretStr):
    #         url = SecretStr(url)
    #     # Validate format as PostgresDsn
    #     # simple validation using PostgresDsn
    #     try:
    #         PostgresDsn(url.get_secret_value())
    #     except Exception as e:
    #         raise ValueError(f"Invalid Postgres URL: {url}") from e
    #     return url


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

    _env_conf = KronicleEnvConf.from_env()
    server: ConnectionSettings = _env_conf.server

    model_config = SettingsConfigDict(
        env_prefix="KRONICLE_",
        env_nested_delimiter="__",
        env_file=".env.kronicle",
        extra="forbid",
        frozen=True,
    )
    _parser = load_ini_file(
        _env_conf.conf_file or DEFAULT_CONF_FILE_PATH if is_file(DEFAULT_CONF_FILE_PATH) else ALT_CONF_FILE_PATH
    )

    # These will be populated from the INI file
    app: AppSettings = AppSettings.from_parser(_parser)
    auth: AuthSettings = AuthSettings.from_parser(_parser)
    jwt: JWTSettings = JWTSettings.from_parser(_parser)
    db: DBSettings = DBSettings(_env_conf)

    @property
    def api_version(self) -> str:
        return "v1"

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

    @property
    def is_prod_env(self) -> bool:
        return self._env_conf.env.is_prod_env

    @property
    def is_dev_env(self) -> bool:
        return self._env_conf.env.is_dev_env

    @property
    def is_stage_env(self) -> bool:
        return self._env_conf.env.is_stage_env
