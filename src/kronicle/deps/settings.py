# kronicle/deps/settings.py
from __future__ import annotations

from typing import Any

from pydantic import PrivateAttr, model_serializer, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from kronicle.deps.settings_env import ConnectionSettings, DBSettings, KronicleEnvConf
from kronicle.deps.settings_ini import AppSettings, AuthSettings, JWTSettings
from kronicle.utils.file_utils import is_file, load_ini_file

# --------------------------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------------------------
DEFAULT_CONF_FILE_PATH = "./conf/default-conf.ini"
ALT_CONF_FILE_PATH = "../conf/default-conf.ini"


# --------------------------------------------------------------------------------------------------
# Kronicle Settings
# --------------------------------------------------------------------------------------------------
class KronicleSettings(BaseSettings):
    """
    Main application configuration. Minimal init, full load via load().

    Supports:
    - Environment variables
    - .env file
    - Nested fields using "__"
    """

    _env_conf: KronicleEnvConf = PrivateAttr()

    server: ConnectionSettings
    db: DBSettings
    app: AppSettings
    jwt: JWTSettings
    auth: AuthSettings

    model_config = SettingsConfigDict(
        env_prefix="KRONICLE_",
        env_nested_delimiter="__",
        env_file=".env.kronicle",
        extra="forbid",
        frozen=True,
    )

    @model_validator(mode="before")
    def load_env_ini(cls, values):
        """Fully initialize Settings with environment and INI."""
        env_conf = KronicleEnvConf.from_env()
        parser = load_ini_file(
            env_conf.conf_file or DEFAULT_CONF_FILE_PATH if is_file(DEFAULT_CONF_FILE_PATH) else ALT_CONF_FILE_PATH
        )
        values["server"] = env_conf.server
        values["db"] = DBSettings(env_conf)

        values["app"] = AppSettings.from_parser(parser)
        values["jwt"] = JWTSettings.from_parser(parser)
        values["auth"] = AuthSettings.from_parser(parser)

        return values

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @model_validator(mode="after")
    def assign_private_env(self):
        # Safe to assign PrivateAttr here
        self._env_conf = KronicleEnvConf.from_env()
        return self

    @model_serializer
    def serialize(self, info):
        return {
            "app": self.app,
            "db": self.db.model_dump(),
            "jwt": self.jwt,
            "auth": self.auth,
            "server": self.server,
        }

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
