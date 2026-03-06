# scripts/utils/read_conf.py
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from dataclasses import dataclass

from asyncpg import connect

from kronicle.utils.str_utils import decode_b64url, normalize_pg_identifier, pad_b64_str, urlsafe_b64decode

"""
Read Kronicle configuration from environment variables.

Expected environment variables:

    KRONICLE_DB_HOST
    KRONICLE_DB_PORT
    KRONICLE_DB_NAME

    KRONICLE_DB_SU_CREDS  : Base64 encoded "db_su_usr:db_su_pwd"
    KRONICLE_CHAN_CREDS   : Base64 encoded "chan_usr:chan_pwd"
    KRONICLE_RBAC_CREDS   : Base64 encoded "rbac_usr:rbac_pwd"

    KRONICLE_SU_INFO      : Base64 encoded "adminuser:admin@example.com:encryptedpwd"
"""

DB_HOST = "KRONICLE_DB_HOST"
DB_PORT = "KRONICLE_DB_PORT"
DB_NAME = "KRONICLE_DB_NAME"
DB_NAME_ALT = "POSTGRES_DB"

DB_SU_CREDS = "KRONICLE_DB_SU_CREDS"  # b64(db_su_usr:db_su_pwd)
DB_SU_NAME = "POSTGRES_USER"
DB_SU_PASS = "POSTGRES_PASSWORD"

CHAN_CREDS = "KRONICLE_CHAN_CREDS"  # b64(chan_usr:chan_pwd)
RBAC_CREDS = "KRONICLE_RBAC_CREDS"  # b64(rbac_usr:rbac_pwd)

APP_SU_INFO = "KRONICLE_SU_INFO"  # b64(adminuser:admin@example.com:encryptedpwd)


@dataclass
class UserCreds:
    username: str
    password: str
    email: str | None = None

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


class DbSuCreds(EnvUserCreds):
    _env = DB_SU_CREDS
    _how = "must be b64(db_su_usr:db_su_pwd)"


@dataclass
class AppSuperuser(EnvUserCreds):
    _env = APP_SU_INFO
    _how = "must be b64(username:email:argon_encrypted_password)"

    @classmethod
    def from_env(cls):
        su_decoded = cls.get_env()
        try:
            su_usr, su_email, su_pwd = su_decoded.split(":", 2)
        except ValueError as e:
            raise RuntimeError(f"{cls._env} {cls._how}") from e
        return AppSuperuser(username=su_usr, email=su_email, password=su_pwd)


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
class KronicleConf:
    db_su: DbSuCreds
    chan_creds: ChanneDbCreds
    rbac_creds: RbacDbCreds
    app_su: AppSuperuser
    db: DbAccess

    @classmethod
    def read_conf(cls) -> KronicleConf:
        pg_usr = os.getenv(DB_SU_NAME, "postgres")
        pg_pwd = os.getenv(DB_SU_PASS, "postgres")
        if pg_usr and pg_pwd:
            db_su = DbSuCreds(username=pg_usr, password=pg_pwd)
        else:
            db_su = DbSuCreds.from_env()

        db_access = DbAccess.from_env(default_creds=db_su)
        return cls(
            db_su=db_su,
            db=db_access,
            chan_creds=ChanneDbCreds.from_env(),
            rbac_creds=RbacDbCreds.from_env(),
            app_su=AppSuperuser.from_env(),
        )


def log_d(here, *args):
    print(f"[{here}]", *args)


if __name__ == "__main__":  # pragma: no cover
    chain = "dG90bzp0YXRh"
    print(decode_b64url(chain))
    print(urlsafe_b64decode(pad_b64_str(chain)).decode("utf-8"))
