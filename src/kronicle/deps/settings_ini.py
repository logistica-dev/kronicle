# kronicle/deps/settings_ini.py
from __future__ import annotations

import importlib.metadata
import tomllib
from configparser import ConfigParser
from importlib.metadata import PackageNotFoundError
from json import dumps
from typing import Any, ClassVar, TypeVar
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

from kronicle.deps.rbac_defaults import RESERVED_NAMES
from kronicle.deps.settings_env import resolve_project_root
from kronicle.utils.dev_logs import log_d
from kronicle.utils.str_utils import strip_quotes

# --------------------------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------------------------
T = TypeVar("T", bound="IniSection")

UNKNOWN_VERSION = "0.0.0"


def package_version() -> str:
    """Resolve the application version.

    Source of truth is the ``version`` field in ``pyproject.toml`` (kept in sync by commitizen);
    falls back to the installed package metadata, then to ``UNKNOWN_VERSION``.
    """
    pyproject_path = resolve_project_root() / "pyproject.toml"
    if pyproject_path.is_file():
        try:
            with pyproject_path.open("rb") as f:
                return tomllib.load(f)["project"]["version"]
        except (OSError, KeyError, tomllib.TOMLDecodeError):
            pass

    try:
        return importlib.metadata.version("kronicle")
    except PackageNotFoundError:
        return UNKNOWN_VERSION


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
        return dumps(self.as_dict(), default=str, **kwargs)

    def __str__(self) -> str:
        return self.json()


class AppSettings(IniSection):
    section = "app"

    version: str = Field(default_factory=package_version)
    name: str = Field(default="Kronicle")
    id: UUID = Field(default_factory=lambda: UUID("ffffffff-62dd-490a-8f7e-b168c68da4a7"))
    description: str = Field(default="FastAPI-powered TimescaleDB microservice for storing time-series measurements")
    openapi_url: str = Field(default="/openapi")
    should_log_request: bool = True

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


class RbacSettings(IniSection):
    section = "rbac"

    reserved_names: list[str] = Field(default=RESERVED_NAMES)
    allow_anonymous: bool = False

    @field_validator("reserved_names", mode="before")
    @classmethod
    def parse_csv_list(cls, v: str | list[str]) -> list[str]:
        if isinstance(v, str):
            return [name.strip() for name in v.split(",") if name.strip()]
        return v


if __name__ == "__main__":  # pragma: no cover
    app_conf = AppSettings()
    log_d("conf_ini", "app_conf", app_conf)
