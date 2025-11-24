# kronicle/controller/db_wrapper.py
import traceback
from asyncio import Lock
from functools import wraps
from typing import Any, Optional
from uuid import UUID

from kronicle.core.ini_settings import conf
from kronicle.db.db_manager import DatabaseManager
from kronicle.db.sensor_metadata import SensorMetadata
from kronicle.types.errors import BadRequestError, DatabaseConnectionError, DatabaseInstructionError, NotFoundError
from kronicle.types.iso_datetime import IsoDateTime
from kronicle.utils.logger import log_e, log_w


def db_error_handler(func):
    """
    Decorator for DatabaseWrapper methods.
    - Raises functional errors (BadRequest, NotFound) from DB operations.
    - Propagates DatabaseConnectionError naturally.
    - Wraps unexpected exceptions into RuntimeError for visibility.
    """

    @wraps(func)
    async def wrapper(instance, *args, **kwargs):
        here = "db_wrp.err_handler"
        try:
            return await func(instance, *args, **kwargs)
        except DatabaseConnectionError as db_err:
            # Let connection-level errors propagate unchanged
            log_e(here, "DB Connection Error", db_err)
            raise
        except (BadRequestError, NotFoundError) as e:
            log_w(here, e.__class__.__name__, e)
            # Functional errors are meaningful to caller; propagate
            raise
        except Exception as e:
            here = "db_error_handler"
            # Log full traceback internally (never send this to client)
            log_e(here, f"Unexpected DB error in {func.__name__}", e)
            log_e(here, "traceback", traceback.format_exc())
            if args:
                log_e(here, "args", *args)
            if kwargs:
                log_e(here, "kwargs", **kwargs)

            # Raise a generic 500-style AppError for the client
            raise DatabaseInstructionError("Unexpected database error") from e

    return wrapper


class DatabaseWrapper:
    """
    Encapsulates all DatabaseManager calls for SensorController.
    Provides functional checks (ensure_metadata / ensure_no_metadata)
    and consistent exception handling.
    """

    _lock = Lock()

    _instance: Optional["DatabaseWrapper"] = None
    _db: DatabaseManager

    def __new__(cls, *args, **kwargs):
        # If singleton already exists, return it
        if cls._instance is not None:
            return cls._instance
        # Otherwise, create a new instance (will be initialized via init_async)
        return super().__new__(cls)

    def __init__(self):
        raise RuntimeError("Misuse of this class, use DatabaseWrapper.init_async()")

    @classmethod
    @db_error_handler
    async def init_async(cls) -> "DatabaseWrapper":
        async with cls._lock:
            if cls._instance is not None:
                return cls._instance

            instance = cls.__new__(cls)  # create instance without calling __init__ directly
            instance._db = DatabaseManager(db_url=conf.db.connection_url, su_url=conf.db.su_url)
            await instance._db.ensure_connection()
            await instance._db.ping()

            cls._instance = instance
            return instance

    # ---------------------------------------------------------
    # Connection checks
    # ---------------------------------------------------------
    @db_error_handler
    async def ping(self) -> bool:
        """Ping the database to ensure connectivity."""
        return await self._db.ping()

    # ---------------------------------------------------------
    # Connection close
    # ---------------------------------------------------------
    @db_error_handler
    async def close_connection(self):
        """Close the underlying DatabaseManager and unset the singleton."""
        if hasattr(self, "_db") and self._db is not None:
            await self._db.close_connection()
        # Unset singleton
        type(self)._instance = None

    # ----------------------------------------
    # Metadata operations
    # ----------------------------------------
    @db_error_handler
    async def fetch_metadata(self, sensor_id: UUID) -> SensorMetadata | None:
        return await self._db.fetch_metadata(sensor_id)

    @db_error_handler
    async def ensure_metadata(self, sensor_id: UUID) -> SensorMetadata:
        meta = await self.fetch_metadata(sensor_id)
        if not meta:
            raise NotFoundError("Sensor metadata not found", details={"sensor_id": str(sensor_id)})
        return meta

    @db_error_handler
    async def ensure_no_metadata(self, sensor_id: UUID) -> None:
        meta = await self.fetch_metadata(sensor_id)
        if meta:
            raise BadRequestError("Sensor metadata already exists", details={"sensor_id": str(sensor_id)})

    @db_error_handler
    async def insert_or_update_metadata(self, metadata: SensorMetadata) -> None:
        await self._db.insert_or_update_metadata(metadata)

    @db_error_handler
    async def fetch_all_metadata(self) -> list[SensorMetadata]:
        return await self._db.fetch_all_metadata()

    @db_error_handler
    async def fetch_metadata_by_name(self, name: str) -> list[SensorMetadata]:
        return await self._db.fetch_metadata_by_name(sensor_name=name)

    @db_error_handler
    async def fetch_metadata_by_tag(self, tag_key: str, tag_value: Any) -> list[SensorMetadata]:
        return await self._db.fetch_metadata_by_tag(tag_key, tag_value)

    @db_error_handler
    async def delete_metadata_and_table(self, sensor_id: UUID, drop_table: bool = True) -> SensorMetadata | None:
        return await self._db.delete_metadata_and_table(sensor_id, drop_table=drop_table)

    @db_error_handler
    async def count_sensor_rows(self, metadata: SensorMetadata) -> int:
        return await self._db.count_sensor_rows(metadata)

    # ----------------------------------------
    # Row operations
    # ----------------------------------------
    @db_error_handler
    async def insert_sensor_rows(self, metadata: SensorMetadata, rows: list[dict]) -> None:
        await self._db.insert_sensor_rows(metadata, rows)

    @db_error_handler
    async def fetch_sensor_rows(
        self,
        metadata: SensorMetadata,
        from_date: IsoDateTime | None = None,
        to_date: IsoDateTime | None = None,
    ) -> list[dict]:
        return await self._db.fetch_sensor_rows(metadata, from_date=from_date, to_date=to_date)

    @db_error_handler
    async def delete_all_rows_for_sensor(self, sensor_id: UUID) -> None:
        await self._db.delete_all_rows_for_sensor(sensor_id)

    # ----------------------------------------
    # Optional operations (setup / clone)
    # ----------------------------------------
    @db_error_handler
    async def clone_metadata(self, source: SensorMetadata, new_metadata: SensorMetadata) -> None:
        await self._db.insert_or_update_metadata(new_metadata)
