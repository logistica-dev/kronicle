# scripts/utils/script_utils.py

from os import getenv

from asyncpg import connect

from kronicle.deps.settings import DBSettings, Settings, get_settings, validate_pg_identifier
from kronicle.utils.dev_logs import log_d
from kronicle.utils.str_utils import decode_b64url


def get_conf() -> Settings:
    conf_file = getenv("KRONICLE_CONF", "./.conf/config.ini")
    conf = get_settings(conf_file)
    if not conf.db.connection_url:
        raise RuntimeError(f"Could not read file '{conf_file}'")
    return conf


def get_su_creds():
    try:
        su_creds_b64 = getenv("KRONICLE_PG_SU_CREDS", "cG9zdGdyZXM6cG9zdGdyZXM")
        su_usr, su_pwd = decode_b64url(su_creds_b64).split(":")
    except Exception as e:
        raise RuntimeError(
            "DB superuser's credentials are not set correctly, expecting base64url encoded usr:pwd"
        ) from e
    return validate_pg_identifier(su_usr), su_pwd


async def get_su_connection(db_conf: DBSettings, db: str | None = None):
    """
    Connect to Postgres as superuser.

    - If `db` is provided, connect to that database.
    - If `db` is None, connect to `db_conf.db_name` if it exists; otherwise fallback to 'postgres'.
    """
    here = "utils.get_su_connection"
    log_d(here, "Retrieving SU creds...")
    su_usr, su_pwd = get_su_creds()

    target_db = db or db_conf.db_name or "postgres"
    log_d(here, f"Connecting to Postgres as superuser on database '{target_db}'...")

    try:
        su_conn = await connect(
            host=db_conf.host,
            port=db_conf.port,
            user=su_usr,
            password=su_pwd,
            database=target_db,
        )
        log_d(here, f"Superuser connection established on '{target_db}'")
        return su_conn
    except Exception as e:
        raise RuntimeError(f"Cannot connect to Postgres as superuser on '{target_db}'") from e
