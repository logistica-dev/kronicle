# kronicle/core/ini_settings.py

import os
from configparser import ConfigParser, ExtendedInterpolation
from dataclasses import dataclass, fields
from json import dumps
from typing import Any, TypeVar

from kronicle.utils.file_utils import check_is_file, expand_file_path
from kronicle.utils.logger import log_d
from kronicle.utils.str_utils import strip_quotes

log_d("-------------------------—------------------------—----------------------[ Launching... ]--")

# --------------------------------------------------------------------------------------------------
T = TypeVar("T", bound="IniSection")


@dataclass(frozen=True)
class IniSection:
    section: str = ""  # subclasses must override

    @classmethod
    def from_parser(cls: type[T], parser: ConfigParser) -> T:
        if not cls.section:
            raise ValueError(f"{cls.__name__} must define a 'section' name")
        kwargs: dict[str, Any] = {}
        for f in fields(cls):
            if f.name == "section":
                continue
            val = parser.get(cls.section, f.name, fallback=f.default)
            kwargs[f.name] = strip_quotes(val)
        return cls(**kwargs)  # type: ignore

    def as_dict(self) -> dict[str, Any]:
        return {f.name: getattr(self, f.name) for f in fields(self) if f.name != "section"}

    def to_json(self, **kwargs) -> str:
        """Return JSON string representation of this section."""
        return dumps(self.as_dict(), **kwargs)

    def __str__(self) -> str:
        return self.to_json()


# --------------------------------------------------------------------------------------------------
# Section-specific settings
# --------------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class JWTSettings(IniSection):
    section: str = "jwt"

    expiration_minutes: int = 5
    algorithm: str = "HS256"
    secret: str = "HS256"


@dataclass(frozen=True)
class DBSettings(IniSection):
    section: str = "db"

    db_usr: str = ""
    db_pwd: str = ""
    db_name: str = ""
    db_address: str = "localhost"
    db_port: int = 5432
    db_connection_url: str = ""
    su_url: str = ""

    @property
    def usr(self):
        return self.db_usr

    @property
    def pwd(self) -> str:
        return self.db_pwd

    @property
    def host(self):
        return self.db_address

    @property
    def port(self) -> int:
        return self.db_port

    @property
    def name(self) -> str:
        return self.db_name

    @property
    def connection_url(self) -> str:
        return f"postgresql://{self.usr}:{self.pwd}@{self.host}:{self.port}/{self.name}"
        # return self.db_connection_url


@dataclass(frozen=True)
class AppSettings(IniSection):
    section: str = "app"

    version: str = "0.0.0"
    name: str = "Kronicle"
    id: str = "ffffffff-62dd-490a-8f7e-b168c68da4a7"
    description: str = "FastAPI-powered TimescaleDB microservice for storing time-series"
    host: str = "localhost"
    port: int | str = 8080
    openapi_url: str = "/openapi"


# --------------------------------------------------------------------------------------------------
# Root settings object
# --------------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class Settings:
    jwt: JWTSettings
    db: DBSettings
    app: AppSettings
    max_retries: int = 10

    @property
    def api_version(self):
        return "v1"

    @classmethod
    def from_parser(cls, parser: ConfigParser) -> "Settings":
        return cls(
            jwt=JWTSettings.from_parser(parser),
            db=DBSettings.from_parser(parser),
            app=AppSettings.from_parser(parser),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "jwt": self.jwt.as_dict(),
            "db": self.db.as_dict(),
            "app": self.app.as_dict(),
            "api_version": self.api_version,
            "max_retries": self.max_retries,
        }

    def to_json(self, **kwargs) -> str:
        """Return JSON string representation of the entire settings."""
        return dumps(self.as_dict(), **kwargs)

    def __str__(self) -> str:
        return self.to_json()


# --------------------------------------------------------------------------------------------------
# Load settings once at runtime
# --------------------------------------------------------------------------------------------------
def load_settings() -> Settings:
    here = "ini.load"
    conf_file = os.getenv("KRONICLE_INI", "../conf/default-conf.ini")
    log_d(here, "Reading conf file", conf_file)
    ini_file = expand_file_path(conf_file)
    check_is_file(ini_file, f"Configuration file not found: '{ini_file}'")
    parser = ConfigParser(interpolation=ExtendedInterpolation())
    parser.read(ini_file)
    return Settings.from_parser(parser)


conf = load_settings()


# --------------------------------------------------------------------------------------------------
# Example usage / debug
# --------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    here = "conf"
    log_d(here, "App name", conf.app.name)
    log_d(here, "JWT expiration (minutes)", conf.jwt.expiration_minutes)
    log_d(here, "DB URL:", conf.db.connection_url)
    log_d(here, "Full config as dict\n", conf.as_dict())
    log_d(here, "Full config as JSON:\n", conf.to_json(indent=2))

    db_url = conf.db.connection_url
    log_d(here, "DB connection url:", db_url)

# if __name__ == "__main__":
#     here = "conf"
#     conf = Settings("../.conf/config.ini")

#     db_url = conf.db.connection_url
#     log_d(here, "DB connection url:", db_url)
#     log_d(here, "app_settings", app_settings)
#     log_d(here, "app name", app_settings.name)
