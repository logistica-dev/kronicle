# kronicle/db/db_manager.py
from contextlib import asynccontextmanager
from json import dumps, loads
from typing import Any, AsyncIterator
from uuid import UUID

from asyncpg import Connection, Pool, connect, create_pool
from asyncpg.exceptions import InvalidAuthorizationSpecificationError, UndefinedTableError, UniqueViolationError

from kronicle.core.ini_settings import conf
from kronicle.db.sensor_metadata import SensorMetadata
from kronicle.db.sensor_schema import SensorSchema
from kronicle.types.errors import DatabaseConnectionError, NotFoundError
from kronicle.types.iso_datetime import IsoDateTime
from kronicle.utils.logger import log_d, log_w

mod = "db_access"

METADATA_TABLE_NAME = "sensor_metadata"


class DatabaseManager:
    """
    Manage PostgreSQL/TimescaleDB connections, metadata persistence, and sensor row insertion/retrieval.
    User data sanitizing is left to the Controller layer.
    """

    _no_conn_err = "Database connection is not active. Use 'async with DatabaseManager(...)'."

    def __init__(
        self,
        db_url: str = conf.db.connection_url,
        su_url: str = conf.db.su_url,
        *,
        use_pool: bool = True,
        min_size: int = 1,
        max_size: int = 10,
    ):
        self.db_url = db_url
        self.su_url = su_url
        self.use_pool = use_pool
        self.min_size = min_size
        self.max_size = max_size

        self._pool: Pool | None = None
        self._conn: Connection | None = None

        # cache of ensured tables for the lifetime of this manager
        self._ensured_tables: set[str] = set()
        self._metadata_ensured: bool = False

    # ----------------------------------------------------------------------------------------------
    # Context manager
    # ----------------------------------------------------------------------------------------------

    async def _enter(self) -> "DatabaseManager":
        """Establish connection or pool and register JSONB codec."""
        if self.use_pool:
            self._pool = await create_pool(
                dsn=self.db_url,
                min_size=self.min_size,
                max_size=self.max_size,
            )
            # Register JSONB codec for all pooled connections
            assert self._pool
            async with self._pool.acquire() as conn:
                await conn.set_type_codec("jsonb", encoder=dumps, decoder=loads, schema="pg_catalog")
        else:
            self._conn = await connect(self.db_url)
            # Register JSONB codec for single connection
            assert self._conn is not None
            await self._conn.set_type_codec("jsonb", encoder=dumps, decoder=loads, schema="pg_catalog")
        log_d(mod, "Database connection established")
        return self

    async def __aenter__(self) -> "DatabaseManager":
        """
        Async context manager entry.
        - Attempts normal connection.
        - On invalid user, bootstraps via superuser and retries.
        """
        try:
            return await self._enter()
        except InvalidAuthorizationSpecificationError:
            # user doesn't exist — use superuser to bootstrap
            log_w(f"{mod}.aenter", f"User '{conf.db.usr}' not found, bootstrapping...")
            su_conn = await connect(self.su_url)
            try:
                # Create user if missing
                if not await su_conn.fetchval("SELECT 1 FROM pg_catalog.pg_user WHERE usename = $1", conf.db.usr):
                    await su_conn.execute(f"CREATE USER {conf.db.usr} WITH PASSWORD '{conf.db.pwd}';")
                    log_d(mod, f"Created user {conf.db.usr}")
                # Create database if missing
                if not await su_conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", conf.db.name):
                    await su_conn.execute(f"CREATE DATABASE {conf.db.name} OWNER {conf.db.usr};")
                    log_d(mod, f"Created database {conf.db.name}")
            finally:
                await su_conn.close()
            log_d(mod, "Retrying normal user connection after bootstrap...")
            # retry as normal user
            return await self._enter()

    async def __aexit__(self, exc_type, exc, tb):
        if self._pool:
            await self._pool.close()
            self._pool = None
            log_d(mod, "Connection pool closed")
        if self._conn:
            await self._conn.close()
            self._conn = None
            log_d(mod, "Single connection closed")

    async def ensure_connection(self):
        """
        Ensure the pool or single connection is established and alive.
        No-op if already initialized.
        """
        if self._pool or self._conn:
            return
        await self.__aenter__()  # safely set up pool or connection

    async def close_connection(self):
        await self.__aexit__(None, None, None)

    # ----------------------------------------------------------------------------------------------
    # Unified connection
    # ----------------------------------------------------------------------------------------------
    @asynccontextmanager
    async def get_connector(self) -> AsyncIterator[Connection]:
        """Yield an active connection (from pool or single)."""
        if self._pool:
            async with self._pool.acquire() as conn:
                yield conn
                return
        if self._conn:
            yield self._conn
            return
        raise DatabaseConnectionError(self._no_conn_err)

    async def ping(self) -> bool:
        """
        Minimal DB connectivity check.
        Returns True if the database is reachable.
        Raises if the connection is down.
        """
        try:
            async with self.get_connector() as conn:
                # Use a trivial query; no table required
                await conn.execute("SELECT 1;")
            return True
        except Exception as e:
            # Optional: log a warning
            log_w("db_manager.ping", f"DB ping failed: {e}")
            raise

    # ----------------------------------------------------------------------------------------------
    # Generic SQL helpers
    # ----------------------------------------------------------------------------------------------
    async def execute(self, sql: str, *params):
        async with self.get_connector() as conn:
            return await conn.execute(sql, *params)

    async def fetch(self, sql: str, *params):
        async with self.get_connector() as conn:
            return await conn.fetch(sql, *params)

    async def fetch_row(self, sql: str, *params):
        async with self.get_connector() as conn:
            return await conn.fetchrow(sql, *params)

    async def fetch_val(self, sql: str, *params):
        async with self.get_connector() as conn:
            return await conn.fetchval(sql, *params)

    async def execute_many(self, sql: str, seq_of_params: list[tuple]):
        async with self.get_connector() as conn:
            return await conn.executemany(sql, seq_of_params)

    # ------------------------------------------------------
    # Table helpers (DDL only on write)
    # -> DDL: data definition language
    # ------------------------------------------------------
    async def _ensure_metadata_table(self, conn: Connection):
        """Ensure sensor_metadata table exists and has required columns (cached)."""
        if self._metadata_ensured:
            return
        here = f"{mod}.ensure_metadata_table"

        existing_cols_rows = await conn.fetch(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{METADATA_TABLE_NAME}';"
        )
        existing_cols = {r["column_name"] for r in existing_cols_rows}

        if not existing_cols:
            # Table does not exist -> create from table_schema
            await conn.execute(f"CREATE TABLE {METADATA_TABLE_NAME} ({SensorMetadata.get_schema_defs()});")
            log_d(here, f"Created {METADATA_TABLE_NAME} table")
        else:
            for col, col_type in SensorMetadata.get_schema_columns():
                if col not in existing_cols:
                    await conn.execute(f"ALTER TABLE {METADATA_TABLE_NAME} ADD COLUMN {col} {col_type};")
                    log_d(here, f"Added column {col} to {METADATA_TABLE_NAME}")

        # ensure extension (no-op if already present)
        await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
        self._metadata_ensured = True

    async def _table_exists(self, conn: Connection, table_name: str) -> bool:
        """Return True if table exists in the current DB."""
        return bool(
            await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1);", table_name
            )
        )

    async def _ensure_sensor_table(
        self,
        conn: Connection,
        sensor_id: UUID,
        schema: SensorSchema,
    ):
        """
        Ensure the TimescaleDB table for a sensor exists.
        Called automatically before insert/update operations.
        """
        here = f"{mod}.ensure_sensor_table"
        table_name = SensorMetadata.get_table_name_for_sensor(sensor_id)
        if table_name in self._ensured_tables:
            cache_exists = True
            exists = True
        else:
            cache_exists = False
            exists = await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1);", table_name
            )

        if not exists:
            table_def = schema.sql_table_definition
            log_d(here, f"Creating new hypertable {table_name}. Col def", table_def)
            await conn.execute(f'CREATE TABLE "{table_name}" ({table_def});')
            await conn.execute(
                "SELECT create_hypertable($1, 'time', if_not_exists => TRUE, migrate_data => TRUE);",
                table_name,
            )
            log_d(here, f"Created new hypertable {table_name}")
        else:
            # Validate existing schema
            # log_d(here, f"Validating existing schema for {table_name}...")
            db_cols = await conn.fetch(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1;",
                table_name,
            )
            db_cols_dict = {r["column_name"]: r["data_type"].upper() for r in db_cols}
            schema.compare_with_db_columns(db_cols_dict)

        if not cache_exists:
            self._ensured_tables.add(table_name)
        return table_name

    # ----------------------------------------------------------------------------------------------
    # Superuser ensure DB & user
    # ----------------------------------------------------------------------------------------------
    async def ensure_user_and_db(self):
        """Ensure database user and DB exist using superuser connection."""
        here = f"{mod}.ensure_user_and_db"
        # log_d(here)
        su_conn = await connect(self.su_url)
        try:
            if not await su_conn.fetchval("SELECT 1 FROM pg_catalog.pg_user WHERE usename = $1", conf.db.usr):
                await su_conn.execute(f"CREATE USER {conf.db.usr} WITH PASSWORD '{conf.db.pwd}';")
                log_d(here, f"User {conf.db.usr} created")

            if not await su_conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", conf.db.name):
                await su_conn.execute(f"CREATE DATABASE {conf.db.name} OWNER {conf.db.usr};")
                log_d(here, f"Database {conf.db.name} created")
        finally:
            await su_conn.close()

    # ----------------------------------------------------------------------------------------------
    # Startup
    # ----------------------------------------------------------------------------------------------
    async def startup(self):
        """Run DB bootstrap: ensure user, DB, metadata table, TimescaleDB extension."""
        here = f"{mod}.startup"
        await self.ensure_user_and_db()
        async with self.get_connector() as conn:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
            await self._ensure_metadata_table(conn)
        log_d(here, "Startup completed: metadata table ensured")

    # ----------------------------------------------------------------------------------------------
    # Metadata helpers
    # ----------------------------------------------------------------------------------------------
    async def fetch_all_metadata(self) -> list[SensorMetadata]:
        here = f"{mod}.fetch_all_metadata"
        try:
            async with self.get_connector() as conn:
                rows = await conn.fetch("SELECT * FROM sensor_metadata ORDER BY received_at DESC")
                return [SensorMetadata.from_db(dict(r)) for r in rows]
        except UndefinedTableError:
            log_w(here, "Table 'sensor_metadata' not found, returning empty list")
            return []

    async def fetch_metadata(self, sensor_id: UUID) -> SensorMetadata | None:
        here = f"{mod}.fetch_metadata"
        try:
            async with self.get_connector() as conn:
                row = await conn.fetchrow("SELECT * FROM sensor_metadata WHERE sensor_id = $1", sensor_id)
                return SensorMetadata.from_db(dict(row)) if row else None
        except UndefinedTableError:
            log_w(here, "Table 'sensor_metadata' not found, returning empty list")
            return None

    async def fetch_metadata_by_tag(self, tag_key: str, tag_value: Any) -> list[SensorMetadata]:
        """
        Fetch metadata entries where tags[tag_key] == tag_value.
        """
        here = f"{mod}.fetch_metadata_by_tag"
        try:
            async with self.get_connector() as conn:
                # JSONB extraction: tags->>'key' = 'value'
                # Cast tag_value to text because JSONB stores everything as JSON
                query = """
                    SELECT * FROM sensor_metadata
                    WHERE tags ->> $1 = $2
                    ORDER BY received_at DESC
                """
                # Ensure tag_value is a string for comparison
                rows = await conn.fetch(query, str(tag_key), str(tag_value))
                # log_d(here, "result", rows)
                return [SensorMetadata.from_db(dict(r)) for r in rows]
        except UndefinedTableError:
            log_w(here, "Table 'sensor_metadata' not found, returning empty list")
            return []

    async def fetch_metadata_by_name(self, sensor_name: str) -> list[SensorMetadata]:
        """
        Fetch the metadata with name = name
        """
        here = f"{mod}.fetch_metadata_by_name"
        try:
            async with self.get_connector() as conn:
                # JSONB extraction: tags->>'key' = 'value'
                # Cast tag_value to text because JSONB stores everything as JSON
                query = """
                    SELECT * FROM sensor_metadata
                    WHERE sensor_name = $1
                    ORDER BY received_at DESC
                """
                # Ensure tag_value is a string for comparison
                rows = await conn.fetch(query, str(sensor_name))
                # log_d(here, "result", rows)
                return [SensorMetadata.from_db(dict(r)) for r in rows]
        except UndefinedTableError:
            log_w(here, "Table 'sensor_metadata' not found, returning empty list")
            return []

    async def _upsert_metadata(self, conn: Connection, metadata: SensorMetadata):
        """Internal helper for insert-or-update behavior."""
        cols = list(SensorMetadata.get_table_schema().keys())
        placeholders = [f"${i+1}" for i in range(len(cols))]
        sql = (
            f"INSERT INTO sensor_metadata ({', '.join(cols)}) "
            f"VALUES ({', '.join(placeholders)}) "
            f"ON CONFLICT (sensor_id) DO UPDATE SET "
            + ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "sensor_id")
        )
        await conn.execute(sql, *metadata.db_ready_values())

    async def create_metadata(self, metadata: SensorMetadata):
        """Insert new sensor metadata. Fail if sensor_id already exists."""
        here = f"{mod}.create_metadata"
        async with self.get_connector() as conn:
            await self._ensure_metadata_table(conn)
            try:
                cols = list(SensorMetadata.get_table_schema().keys())
                placeholders = [f"${i+1}" for i in range(len(cols))]
                sql = f"INSERT INTO sensor_metadata ({', '.join(cols)}) VALUES ({', '.join(placeholders)})"
                await conn.execute(sql, *metadata.db_ready_values())
                log_d(here, f"Created metadata for sensor {metadata.sensor_id}")
            except UniqueViolationError:
                log_w(here, f"Metadata for sensor {metadata.sensor_id} already exists")
                raise

    async def update_metadata(self, metadata: SensorMetadata):
        """Update existing sensor metadata. Fail if sensor_id does not exist."""
        here = f"{mod}.update_metadata"
        async with self.get_connector() as conn:
            await self._ensure_metadata_table(conn)
            existing = await conn.fetchrow("SELECT 1 FROM sensor_metadata WHERE sensor_id = $1", metadata.sensor_id)
            if not existing:
                log_w(here, f"Tried to update metadata for non-existent sensor {metadata.sensor_id}")
                raise NotFoundError("Sensor metadata not found", details={"sensor_id": str(metadata.sensor_id)})

            cols = [c for c in SensorMetadata.get_table_schema().keys() if c != "sensor_id"]
            set_expr = ", ".join(f"{c} = ${i+2}" for i, c in enumerate(cols))
            sql = f"UPDATE sensor_metadata SET {set_expr} WHERE sensor_id = $1"
            values = metadata.db_ready_values()
            await conn.execute(sql, metadata.sensor_id, *values[1:])
            log_d(here, f"Updated metadata for sensor {metadata.sensor_id}")

    async def insert_or_update_metadata(self, metadata: SensorMetadata):
        """Compatibility wrapper (kept for now). Always upserts."""
        async with self.get_connector() as conn:
            await self._ensure_metadata_table(conn)
            await self._upsert_metadata(conn, metadata)

    # ----------------------------------------------------------------------------------------------
    # Sensor data helpers
    # ----------------------------------------------------------------------------------------------

    async def count_sensor_rows(self, metadata: SensorMetadata) -> int:
        """
        Return the total number of rows for a given sensor.
        """
        # here = f"{mod}.count_sensor_rows"
        table_name = metadata.data_table_name
        sql = f'SELECT COUNT(*) FROM "{table_name}"'
        try:
            async with self.get_connector() as conn:
                result = await conn.fetchval(sql)
                return int(result) if result is not None else 0
        except UndefinedTableError:
            # log_w(here, f"Table '{table_name}' not found, returning 0")
            return 0

    async def insert_sensor_rows(
        self,
        metadata: SensorMetadata,
        rows: list[dict[str, Any]],
        *,
        update_metadata: bool = True,
    ):
        """
        Insert multiple rows into a sensor table.
        - Ensures metadata exists (insert or update depending on `update_metadata`).
        - Creates table if needed.
        - Skips invalid rows (validated by schema).
        """
        if not rows:
            return
        here = f"{mod}.insert_sensor_rows"
        async with self.get_connector() as conn:
            # --- Ensure metadata first ---
            await self._ensure_metadata_table(conn)
            sensor_id = metadata.sensor_id
            sensor_schema = metadata.sensor_schema
            existing = await conn.fetchrow("SELECT 1 FROM sensor_metadata WHERE sensor_id = $1", sensor_id)

            if not existing:
                await self._upsert_metadata(conn, metadata)
                log_w(here, f"Created missing metadata entry for sensor {sensor_id}")
            elif update_metadata:
                await self._upsert_metadata(conn, metadata)
                log_w(here, f"Updated metadata for sensor {sensor_id}")
            else:
                log_d(here, f"Metadata exists for sensor {sensor_id}, not updated (update_metadata=False)")

            # --- Ensure sensor table ---
            await self._ensure_sensor_table(conn, sensor_id, sensor_schema)

            # --- Prepare insert ---
            table_name = metadata.data_table_name
            columns = sensor_schema.ordered_columns
            placeholders = [f"${i+1}" for i in range(len(columns))]
            sql = f'INSERT INTO "{table_name}" ({", ".join(columns)}) VALUES ({", ".join(placeholders)})'
            values_list, errors = sensor_schema.rows_to_db_tuples(rows, log_errors=True)
            if errors:
                log_w(here, f"Skipped {len(errors)} invalid rows for {table_name}: {errors}")
            if values_list:
                try:
                    await conn.executemany(sql, values_list)
                    log_d(mod, f"Inserted {len(values_list)} valid rows into {table_name}")
                except Exception as e:
                    log_w(here, f"ERR with 'f{sql}' with vals {values_list}", e)

    async def fetch_sensor_rows(
        self,
        metadata: SensorMetadata,
        from_date: IsoDateTime | None = None,
        to_date: IsoDateTime | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """
        Fetch sensor rows as parsed dicts using SensorSchema.parse_row.
        (Read-only, assumes table exists)
        """
        here = f"{mod}.fetch_sensor_rows"
        table_name = metadata.data_table_name
        where_clauses, params = [], []
        idx = 1

        if from_date:
            where_clauses.append(f"time >= ${idx}")
            params.append(from_date)
            idx += 1
        if to_date:
            where_clauses.append(f"time <= ${idx}")
            params.append(to_date)
            idx += 1

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        limit_sql = f"LIMIT {limit}" if limit else ""
        sql_req = f'SELECT * FROM "{table_name}" {where_sql} ORDER BY time ASC {limit_sql}'
        try:
            async with self.get_connector() as conn:
                rows = await conn.fetch(sql_req, *params)
                return [metadata.sensor_schema.validate_row(dict(r), from_user=False) for r in rows]
        except UndefinedTableError:
            log_w(here, f"Table '{table_name}' not found, returning empty list")
            return []

    # ----------------------------------------------------------------------------------------------
    # Deletion helpers
    # ----------------------------------------------------------------------------------------------
    async def delete_metadata_and_table(self, sensor_id: UUID, *, drop_table: bool = True):
        """
        Delete metadata row and (optionally) drop the corresponding sensor table.
        - Removes the table from in-memory cache.
        """
        table_name = SensorMetadata.get_table_name_for_sensor(sensor_id)
        async with self.get_connector() as conn:
            # Delete metadata row
            await conn.execute("DELETE FROM sensor_metadata WHERE sensor_id = $1", sensor_id)

            if drop_table:
                # Use IF EXISTS to avoid raising when table already gone.
                await conn.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
                # For safety, remove from ensured cache if present
                self._ensured_tables.discard(table_name)

    async def delete_all_rows_for_sensor(self, sensor_id: UUID) -> None:
        """
        Delete all rows for a given sensor.
        Metadata table entry is untouched.
        """
        here = f"{mod}.delete_all_rows"
        metadata = await self.fetch_metadata(sensor_id)
        if not metadata:
            log_w(here, f"Sensor metadata not found for sensor_id={sensor_id}")
            raise NotFoundError("Sensor metadata not found", details={"sensor_id": str(sensor_id)})

        table_name = metadata.data_table_name
        count_sql = f'SELECT COUNT(*) FROM "{table_name}"'
        delete_sql = f'DELETE FROM "{table_name}"'

        try:
            async with self.get_connector() as conn:
                # count rows before deletion
                row_count: int = await conn.fetchval(count_sql) or 0
                if row_count == 0:
                    log_d(here, f"No rows to delete in {table_name} for sensor_id={sensor_id}")
                    return

                # delete rows
                result = await conn.execute(delete_sql)
                # optionally check the command tag matches row_count
                try:
                    deleted_count = int(result.split()[1])
                    if deleted_count != row_count:
                        log_w(
                            here,
                            f"Deleted rows ({deleted_count}) != counted rows ({row_count}) for {table_name}",
                        )
                except Exception:
                    log_w(here, f"Could not verify deletion count for {table_name}, command tag: {result}")

                log_d(here, f"Deleted {row_count} rows from {table_name} for sensor_id={sensor_id}")
        except UndefinedTableError:
            log_w(here, f"Table '{table_name}' not found, nothing to delete")
        except Exception as e:
            log_w(here, f"Error deleting rows from {table_name} for sensor_id={sensor_id}: {e}")
            raise


# --------------------------------------------------------------------------------------------------
# Quick main test
# --------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    import asyncio
    import random
    from datetime import timedelta, timezone
    from uuid import uuid4

    from kronicle.db.sensor_metadata import SensorMetadata
    from kronicle.db.sensor_schema import SensorSchema
    from kronicle.utils.logger import log_d

    db_mgr: DatabaseManager

    async def test_metadata_workflows():
        here = f"{mod}.test_metadata_workflows"
        log_d(here)

        async with DatabaseManager() as db_mgr:
            await db_mgr.startup()
            log_d(here, "Startup completed")

            base_time = IsoDateTime.now(timezone.utc)

            # --------------------------------------------------------------------------------------
            # Define sensor schema
            # --------------------------------------------------------------------------------------
            json_schema = {"temperature": "number", "humidity": "number", "meta": "json"}
            sensor_schema = SensorSchema.from_user_json(json_schema)

            # --------------------------------------------------------------------------------------
            # Workflow 1: Explicit creation
            # --------------------------------------------------------------------------------------
            sensor_id1 = uuid4()
            metadata1 = SensorMetadata(
                sensor_id=sensor_id1,
                sensor_schema=sensor_schema,
                metadata={"location": "lab", "unit": "Celsius"},
                tags={"room": 101},
            )
            await db_mgr.create_metadata(metadata1)
            log_d(here, f"Explicitly created metadata for sensor {sensor_id1}")

            rows1 = [
                {
                    "time": base_time + timedelta(seconds=i),
                    "temperature": random.uniform(20, 30),
                    "humidity": random.uniform(30, 60),
                    "meta": {"note": f"reading {i}"},
                }
                for i in range(3)
            ]
            await db_mgr.insert_sensor_rows(metadata1, rows1)
            log_d(here, f"Inserted {len(rows1)} rows for sensor {sensor_id1}")

            # --------------------------------------------------------------------------------------
            # Workflow 2: Implicit creation
            # --------------------------------------------------------------------------------------
            sensor_id2 = uuid4()
            metadata2 = SensorMetadata(
                sensor_id=sensor_id2,
                sensor_schema=sensor_schema,
                metadata={"location": "office"},
                tags={},
            )
            # Insert rows without prior metadata creation
            rows2 = [
                {
                    "time": base_time + timedelta(seconds=i),
                    "temperature": random.uniform(15, 25),
                    "humidity": random.uniform(30, 50),
                    "meta": {},
                }
                for i in range(2)
            ]
            await db_mgr.insert_sensor_rows(metadata2, rows2)
            log_d(here, f"Inserted {len(rows2)} rows for sensor {sensor_id2} (metadata auto-created)")

            # --------------------------------------------------------------------------------------
            # Workflow 3: Update existing metadata
            # --------------------------------------------------------------------------------------
            metadata1.metadata["unit"] = "Kelvin"  # type: ignore
            metadata1.tags["floor"] = 1  # type: ignore
            await db_mgr.update_metadata(metadata1)
            log_d(here, f"Updated metadata for sensor {sensor_id1}")

            # --------------------------------------------------------------------------------------
            # Fetch and log all metadata
            # --------------------------------------------------------------------------------------
            all_metadata = await db_mgr.fetch_all_metadata()
            for m in all_metadata:
                log_d(here, f"Metadata entry: {m}")

            # --------------------------------------------------------------------------------------
            # Fetch rows
            # --------------------------------------------------------------------------------------
            rows_fetched1 = await db_mgr.fetch_sensor_rows(metadata1)
            log_d(here, f"Fetched {len(rows_fetched1)} rows for sensor {sensor_id1}")

            rows_fetched2 = await db_mgr.fetch_sensor_rows(metadata2)
            log_d(here, f"Fetched {len(rows_fetched2)} rows for sensor {sensor_id2}")

    async def main():
        here = f"{mod}.test_ok"

        async with DatabaseManager() as db_mgr:
            # --------------------------------------------------------------------------------------
            # Startup: ensure DB, metadata table
            # --------------------------------------------------------------------------------------
            await db_mgr.startup()
            log_d(here, "Startup completed")

            # --------------------------------------------------------------------------------------
            # Create sensor schema and metadata
            # --------------------------------------------------------------------------------------
            json_schema = {"temperature": "number", "humidity": "number", "meta": "json"}
            sensor_schema = SensorSchema.from_user_json(json_schema)
            sensor_id = uuid4()
            metadata = SensorMetadata(
                sensor_id=sensor_id,
                sensor_schema=sensor_schema,
                metadata={"location": "lab", "unit": "Celsius"},
                tags={"room": 101},
            )

            # Insert or update sensor metadata
            await db_mgr.insert_or_update_metadata(metadata)
            log_d(here, "Metadata inserted/updated")

            # --------------------------------------------------------------------------------------
            # Insert sample rows
            # --------------------------------------------------------------------------------------
            base_time = IsoDateTime.now(timezone.utc)

            # Normal rows
            rows = [
                {
                    "time": base_time + timedelta(seconds=i),
                    "temperature": random.uniform(20, 30),
                    "humidity": random.uniform(30, 60),
                    "meta": {"note": f"reading {i}"},
                }
                for i in range(5)
            ]

            # Edge case: missing optional 'meta', empty JSONB
            rows.append(
                {
                    "time": base_time + timedelta(seconds=5),
                    "temperature": 25.0,
                    "humidity": 45.0,
                    "meta": {},  # empty JSONB
                }
            )

            # Edge case: large JSONB
            rows.append(
                {
                    "time": base_time + timedelta(seconds=6),
                    "temperature": 22.0,
                    "humidity": 50.0,
                    "meta": {"values": list(range(100))},
                }
            )

            await db_mgr.insert_sensor_rows(metadata, rows)
            log_d(here, f"{len(rows)} rows inserted for sensor {sensor_id}")

            # --------------------------------------------------------------------------------------
            # Fetch rows using sensor schema
            # --------------------------------------------------------------------------------------
            fetched_rows = await db_mgr.fetch_sensor_rows(metadata)
            for idx, row in enumerate(fetched_rows):
                log_d(here, f"Fetched row {idx}", row)

            # --------------------------------------------------------------------------------------
            # Fetch rows with time filter
            # --------------------------------------------------------------------------------------
            from_date = base_time + timedelta(seconds=2)
            to_date = base_time + timedelta(seconds=5)
            filtered_rows = await db_mgr.fetch_sensor_rows(metadata, from_date=from_date, to_date=to_date)
            log_d(here, f"Rows fetched between {from_date} and {to_date}", filtered_rows)

            # --------------------------------------------------------------------------------------
            # Fetch metadata
            # --------------------------------------------------------------------------------------
            fetched_metadata = await db_mgr.fetch_metadata(sensor_id)
            log_d(here, "Fetched metadata", fetched_metadata)

            tagged_metadata = await db_mgr.fetch_metadata_by_tag(tag_key="Test", tag_value="True")
            log_d(here, "Tagged metadata", tagged_metadata)

            # --------------------------------------------------------------------------------------
            # Insert metadata with empty tags
            # --------------------------------------------------------------------------------------
            metadata_empty_tags = SensorMetadata(
                sensor_id=uuid4(),
                sensor_schema=sensor_schema,
                metadata={"location": "office"},
                tags={},  # empty tags
            )
            await db_mgr.insert_or_update_metadata(metadata_empty_tags)
            fetched_empty_tags = await db_mgr.fetch_metadata(metadata_empty_tags.sensor_id)
            log_d(here, "Metadata with empty tags", fetched_empty_tags)

    async def main_with_null_cases():
        here = f"{mod}.test_err"
        log_d(here)

        async with DatabaseManager() as db_mgr:
            # --------------------------------------------------------------------------------------
            # Startup: ensure DB and metadata table
            # --------------------------------------------------------------------------------------
            await db_mgr.startup()
            log_d(here, "Startup completed")

            # --------------------------------------------------------------------------------------
            # Sensor schema & metadata
            # --------------------------------------------------------------------------------------
            json_schema = {"temperature": "number", "humidity": "number", "meta": "json"}
            sensor_schema = SensorSchema.from_user_json(json_schema)
            sensor_id = uuid4()
            metadata = SensorMetadata(
                sensor_id=sensor_id,
                sensor_schema=sensor_schema,
                metadata=None,  # test empty metadata
                tags=None,  # test empty tags
            )
            await db_mgr.insert_or_update_metadata(metadata)
            log_d(here, "Metadata inserted/updated (empty metadata/tags)")

            # --------------------------------------------------------------------------------------
            # Insert valid rows
            # --------------------------------------------------------------------------------------
            base_time = IsoDateTime.now(timezone.utc)
            valid_rows = [
                {
                    "time": base_time + timedelta(seconds=i),
                    "temperature": random.uniform(20, 30),
                    "humidity": random.uniform(30, 60),
                    "meta": {"note": f"reading {i}"},
                }
                for i in range(3)
            ]

            # --------------------------------------------------------------------------------------
            # Insert invalid rows for robustness testing
            # --------------------------------------------------------------------------------------
            invalid_rows = [
                {"time": base_time, "temperature": "hot", "humidity": 50, "meta": {}},  # wrong type
                {"time": base_time, "humidity": 40, "meta": {}},  # missing column
                {"time": base_time, "temperature": 25.0, "humidity": 50, "meta": "not_json"},  # invalid JSONB
            ]
            all_rows = valid_rows + invalid_rows

            try:
                # Convert rows to DB tuples (logs errors, skips invalid)
                values_list, errors = metadata.sensor_schema.rows_to_db_tuples(all_rows, log_errors=True)
                if values_list:
                    await db_mgr.insert_sensor_rows(metadata, all_rows)
                    log_d(here, f"{len(valid_rows)} valid rows inserted (invalid rows skipped)")
                if errors:
                    raise ValueError(f"{len(errors)} rows invalid: {errors}")

            except Exception as e:
                log_d(here, f"Error during insertion: {e}")

            # --------------------------------------------------------------------------------------
            # Fetch all rows
            # --------------------------------------------------------------------------------------
            fetched_rows = await db_mgr.fetch_sensor_rows(metadata)
            log_d(here, f"Fetched {len(fetched_rows)} rows after insertion")
            for idx, row in enumerate(fetched_rows):
                log_d(here, f"Row {idx}", row)

            # --------------------------------------------------------------------------------------
            # Fetch metadata
            # --------------------------------------------------------------------------------------
            fetched_metadata = await db_mgr.fetch_metadata(sensor_id)
            log_d(here, "Fetched metadata", fetched_metadata)

            # --------------------------------------------------------------------------------------
            # Test time filtering
            # --------------------------------------------------------------------------------------
            from_date = base_time + timedelta(seconds=1)
            to_date = base_time + timedelta(seconds=2)
            filtered_rows = await db_mgr.fetch_sensor_rows(metadata, from_date=from_date, to_date=to_date)
            log_d(here, f"Fetched {len(filtered_rows)} rows between {from_date} and {to_date}")
            for idx, row in enumerate(filtered_rows):
                log_d(here, f"Filtered Row {idx}", row)

            # --------------------------------------------------------------------------------------
            # Test fetch_metadata_table
            # --------------------------------------------------------------------------------------
            all_metadata = await db_mgr.fetch_all_metadata()
            log_d(here, f"Total metadata entries: {len(all_metadata)}")
            for m in all_metadata:
                log_d(here, "Metadata entry", m)

    async def query_db_test():
        here = "query_db_test"
        async with DatabaseManager() as db_mgr:
            await db_mgr.startup()
            log_d(here, "Startup completed")
            all_metadata = await db_mgr.fetch_all_metadata()
            for m in all_metadata:
                log_d(here, "Metadata entry", m)
            log_d(here, f"Total metadata entries: {len(all_metadata)}")

            tagged_metadata = await db_mgr.fetch_metadata_by_tag(tag_key="Test", tag_value=1)
            log_d(here, "Tagged metadata", tagged_metadata)

    async def main_tests():
        await main()
        await test_metadata_workflows()
        await main_with_null_cases()

    asyncio.run(query_db_test())
