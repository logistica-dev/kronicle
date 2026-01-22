# kronicle/utils/asyncpg_utils.py
from re import match

from asyncpg import Connection, connect

from kronicle.utils.dev_logs import log_d

mod = "asyncpg_utils"


def parse_pg_dsn(dsn: str) -> tuple[str, str, str]:
    """
    Parse a PostgreSQL DSN and return (db_user, host_port, db_name).

    Supports DSNs of the form:
        postgresql://user:password@host:port/dbname
        postgresql://user:password@host/dbname
    """
    pattern = (
        r"postgresql://"  # scheme
        r"(?P<user>[^:]+):[^@]+@"  # user and skip password
        r"(?P<host>[^:/]+)"  # host
        r"(:(?P<port>\d+))?"  # optional :port
        r"/(?P<dbname>[^/?]+)"  # db name
    )

    match_res = match(pattern, dsn)
    if not match_res:
        raise ValueError(f"Invalid PostgreSQL DSN: {dsn}")

    user = match_res.group("user")
    host = match_res.group("host")
    port = match_res.group("port") or "5432"  # default Postgres port
    dbname = match_res.group("dbname")

    return user, f"{host}:{port}", dbname


async def verify_connection(dsn: str):
    """
    Check that the app user can connect to the DB.
    Returns (db_usr, host_port, db_name)
    """
    here = f"{mod}.verify_connection"
    if not dsn:
        raise ValueError("Input connection string should not be null")
    try:
        db_usr, host, db_name = parse_pg_dsn(dsn)
    except Exception as e:
        raise ValueError(
            "Input connection string is malformed, should be 'postgresql://alice:secret@localhost:5432/mydb'"
        ) from e

    try:
        conn = await connect(dsn)
        await conn.close()
        log_d(here, f"Verified: app user '{db_usr}' can connect to DB '{db_name}'")
    except Exception as e:
        raise ConnectionError(f"Cannot connect to DB '{db_name}' as user '{db_usr}': {e}") from e

    return db_usr, host, db_name


async def table_exists(conn: Connection, namespace: str, table_name: str) -> bool:
    """Return True if the table exists."""
    exists = await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = $2
        )
        """,
        namespace,
        table_name,
    )
    return bool(exists)


# Example usage
if __name__ == "__main__":  # pragma: no cover
    dsn1 = "postgresql://alice:secret@localhost:5432/mydb"
    dsn2 = "postgresql://bob:pass@dbserver/myotherdb"

    for dsn in [dsn1, dsn2]:
        user, host_port, dbname = parse_pg_dsn(dsn)
        print(user, host_port, dbname)
