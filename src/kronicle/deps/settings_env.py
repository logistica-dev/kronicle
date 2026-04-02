# src/kronicle/deps/settings_env.py
from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from json import dumps
from os import getenv
from typing import Any

from asyncpg import connect
from pydantic import SecretStr

from kronicle.utils.dev_logs import log_e, log_w
from kronicle.utils.str_utils import decode_b64url, normalize_pg_identifier

"""
Read Kronicle configuration from environment variables.

Expected environment variables:

    KRONICLE_DB_HOST
    KRONICLE_DB_PORT
    KRONICLE_DB_NAME

    KRONICLE_CHAN_CREDS   : Base64 encoded "chan_usr:chan_pwd"
    KRONICLE_RBAC_CREDS   : Base64 encoded "rbac_usr:rbac_pwd"

"""

# --------------------------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------------------------
DB_HOST = "KRONICLE_DB_HOST"
DB_PORT = "KRONICLE_DB_PORT"
DB_NAME = "KRONICLE_DB_NAME"
DB_NAME_ALT = "POSTGRES_DB"

APP_HOST = "KRONICLE_HOST"
APP_PORT = "KRONICLE_PORT"

CHAN_CREDS = "KRONICLE_CHAN_CREDS"  # b64(chan_usr:chan_pwd)
RBAC_CREDS = "KRONICLE_RBAC_CREDS"  # b64(rbac_usr:rbac_pwd)


KRONICLE_CONF = "KRONICLE_CONF"
KRONICLE_ENV = "KRONICLE_ENV"


# --------------------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------------------


def get_env_var(env_var: str, default_val: Any):
    here = "get_env_var"
    env_val = getenv(env_var)
    if env_val is None:
        log_w(here, f"Environment variable not set: {env_var}, defaulting to {default_val}")
        return default_val
    return env_val


def ensure_env_var(env_var: str):
    here = "ensure_env_var"
    try:
        env_val = getenv(env_var)
        if not env_val:
            raise RuntimeError(f"Environment variable empty: {env_var}")
        return env_val
    except Exception as e:
        log_e(here, f"Environment variable not set: {env_var}")
        raise RuntimeError(f"Environment variable not set: {env_var}") from e


@dataclass
class UserCreds:
    username: str
    password: str

    @property
    def creds(self):
        return self.username, self.password

    @classmethod
    def from_env(cls, env_usr: str, env_pwd: str) -> UserCreds:
        usr = getenv(env_usr)
        pwd = getenv(env_pwd)
        if not usr or not pwd:
            raise RuntimeError(f"Not found: {env_usr} & {env_pwd}")
        return cls(username=usr, password=pwd)


@dataclass
class EnvUserCreds(UserCreds):
    _env: str = ""
    _how: str = "must be b64(username:password)"

    @classmethod
    def get_env(cls) -> str:
        env_var = getenv(cls._env)
        if not env_var:
            raise RuntimeError(f"Not found: {cls._env} {cls._how}")
        return decode_b64url(env_var)

    @classmethod
    def from_env(cls, *args, **kwargs):
        try:
            decoded = cls.get_env()
            usr, pwd = decoded.split(":", 1)
        except ValueError as e:
            raise RuntimeError(f"{cls._env} {cls._how}") from e
        return cls(username=normalize_pg_identifier(usr), password=pwd)


class ChanneDbCreds(EnvUserCreds):
    _env = CHAN_CREDS
    _how = "must be b64(chan_usr:chan_pwd)"


class RbacDbCreds(EnvUserCreds):
    _env = RBAC_CREDS
    _how = "must be b64(rbac_usr:rbac_pwd)"


@dataclass
class ConnectionSettings:
    host: str
    port: int

    @classmethod
    def from_env(cls, host_var: str = APP_HOST, port_var: str = APP_PORT, *args, **kwargs) -> ConnectionSettings:
        host = get_env_var(host_var, "0.0.0.0")
        port = int(get_env_var(port_var, 8080))
        return cls(host=host, port=port)


@dataclass
class DbAccess(ConnectionSettings):
    name: str
    usr: str
    pwd: str

    @classmethod
    def from_env(cls, default_creds: UserCreds) -> DbAccess:
        host = ensure_env_var(DB_HOST)
        port = int(ensure_env_var(DB_PORT))
        try:
            name = getenv(DB_NAME_ALT) or get_env_var(DB_NAME, "kronicle")
            name = normalize_pg_identifier(name)
            usr = normalize_pg_identifier(default_creds.username)
        except ValueError as e:
            raise RuntimeError(e) from e
        return cls(
            host=host,
            port=port,
            name=name,
            usr=usr,
            pwd=default_creds.password,
        )

    def dsn(self, creds: UserCreds | None = None, db_name: str | None = None) -> str:
        usr = creds.username if creds else self.usr
        pwd = creds.password if creds else self.pwd
        return f"postgresql://{usr}:{pwd}@{self.host}:{self.port}/{db_name or self.name}"

    @asynccontextmanager
    async def session(
        self,
        creds: UserCreds | None = None,
        db_name: str | None = None,
    ):
        db_usr = creds.username if creds else self.usr
        db_pwd = creds.password if creds else self.pwd
        db_name = db_name or self.name

        conn = await connect(
            host=self.host,
            port=self.port,
            user=db_usr,
            password=db_pwd,
            database=db_name,
        )
        try:
            yield conn
        finally:
            await conn.close()


@dataclass
class AppEnv:
    _ENV_DEV = "dev"
    _ENV_PROD = "prod"
    _ENV_STAGE = "stage"
    _env: str = _ENV_PROD

    @classmethod
    def from_env(cls) -> AppEnv:
        kronicle_env = get_env_var(KRONICLE_ENV, cls._ENV_PROD).lower().strip()
        mapping = {
            "test": cls._ENV_DEV,
            "dev": cls._ENV_DEV,
            "development": cls._ENV_DEV,
            "prod": cls._ENV_PROD,
            "production": cls._ENV_PROD,
            "stage": cls._ENV_STAGE,
        }
        if kronicle_env not in mapping:
            raise ValueError(f"Invalid KRONICLE_ENV value: '{kronicle_env}'. Must be one of {list(mapping.values())}")
        return AppEnv(_env=mapping[kronicle_env])

    @property
    def is_dev_env(self) -> bool:
        return self._env == self._ENV_DEV

    @property
    def is_prod_env(self) -> bool:
        return self._env == self._ENV_PROD

    @property
    def is_stage_env(self) -> bool:
        return self._env == self._ENV_STAGE


@dataclass
class KronicleEnvConf:
    chan_creds: ChanneDbCreds
    rbac_creds: RbacDbCreds
    db: DbAccess
    server: ConnectionSettings
    env: AppEnv
    conf_file: str | None

    @classmethod
    def from_env(cls) -> KronicleEnvConf:
        rbac_creds = RbacDbCreds.from_env()
        chan_creds = ChanneDbCreds.from_env()

        db_access = DbAccess.from_env(default_creds=chan_creds)
        app_server = ConnectionSettings.from_env()
        app_env = AppEnv.from_env()
        conf_file: str | None = getenv(KRONICLE_CONF)
        return cls(
            server=app_server,
            db=db_access,
            rbac_creds=rbac_creds,
            chan_creds=chan_creds,
            env=app_env,
            conf_file=conf_file,
        )


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

    def model_dump(self, **kwargs) -> dict[str, str | None]:
        return {"connection_url": self.masked_connection_url}

    def model_dump_json(self, **kwargs) -> str:
        return dumps(self.model_dump(), indent=2)

    def __str__(self) -> str:
        return f"DBSettings({self.masked_connection_url})"

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
