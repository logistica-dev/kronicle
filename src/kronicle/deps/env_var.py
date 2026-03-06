# kronicle/deps/env_var.py

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from dataclasses import dataclass

from asyncpg import connect

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

DB_HOST = "KRONICLE_DB_HOST"
DB_PORT = "KRONICLE_DB_PORT"
DB_NAME = "KRONICLE_DB_NAME"
DB_NAME_ALT = "POSTGRES_DB"

CHAN_CREDS = "KRONICLE_CHAN_CREDS"  # b64(chan_usr:chan_pwd)
RBAC_CREDS = "KRONICLE_RBAC_CREDS"  # b64(rbac_usr:rbac_pwd)


@dataclass
class UserCreds:
    username: str
    password: str

    @property
    def creds(self):
        return self.username, self.password

    @classmethod
    def from_env(cls, env_usr: str, env_pwd: str) -> UserCreds:
        usr = os.getenv(env_usr)
        pwd = os.getenv(env_pwd)
        if not usr or not pwd:
            raise RuntimeError(f"Not found: {env_usr} & {env_pwd}")
        return cls(username=usr, password=pwd)


@dataclass
class EnvUserCreds(UserCreds):
    _env: str = ""
    _how: str = "must be b64(username:password)"

    @classmethod
    def get_env(cls) -> str:
        env_var = os.getenv(cls._env)
        if not env_var:
            raise RuntimeError(f"Not found: {cls._env} {cls._how}")
        return decode_b64url(env_var)

    @classmethod
    def from_env(cls, *args, **kwargs):
        decoded = cls.get_env()
        try:
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
class DbAccess:
    host: str
    port: int
    name: str
    usr: str
    pwd: str

    @classmethod
    def _get_env(cls, env_var: str):
        try:
            env_val = os.getenv(env_var)
            if not env_val:
                raise RuntimeError(f"Environment variable empty: {env_var}")
            return env_val
        except Exception as e:
            raise RuntimeError(f"Environment variable not set: {env_var}") from e

    @classmethod
    def from_env(cls, default_creds: UserCreds) -> DbAccess:
        host = cls._get_env(DB_HOST)
        port = int(cls._get_env(DB_PORT))
        name = os.getenv(DB_NAME) or os.getenv(DB_NAME_ALT, "kronicle")
        return DbAccess(
            host=host,
            port=port,
            name=normalize_pg_identifier(name),
            usr=default_creds.username,
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
class KronicleDbConf:
    chan_creds: ChanneDbCreds
    rbac_creds: RbacDbCreds
    db: DbAccess

    @classmethod
    def from_env(cls) -> KronicleDbConf:
        rbac_creds = RbacDbCreds.from_env()
        chan_creds = ChanneDbCreds.from_env()

        db_access = DbAccess.from_env(default_creds=chan_creds)
        return cls(db=db_access, rbac_creds=rbac_creds, chan_creds=chan_creds)
