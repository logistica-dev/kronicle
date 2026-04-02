# kronicle/db/base/kronicle_base.py
from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import DateTime, func, inspect, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, declarative_base, mapped_column

from kronicle.utils.dev_logs import log_block, log_e

mod = "kron_base"

Base = declarative_base()


class KronicleBase(Base):
    """
    Base class for all Core and RBAC tables.
    Provides:
      - Primary UUID key
      - Created/Updated timestamps
      - Flexible JSONB details
      - Table structure validation at startup
    """

    __abstract__ = True  # Do not create a table for this class itself

    @classmethod
    def namespace(cls):
        raise NotImplementedError("Method namespace() should be implemented at lower levels")

    @classmethod
    def tablename(cls):
        if hasattr(cls, "__tablename__"):
            return cls.__tablename__
        raise NotImplementedError("This is most likely a abstract class and doesn't map to a table")

    @classmethod
    def table(cls):
        return f"{cls.namespace()}.{cls.tablename()}"

    # Primary key UUID
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    # Flexible JSONB for per-record extra info
    details: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,  # Python-side default
        server_default=text("'{}'::jsonb"),  # PostgreSQL default
    )

    @classmethod
    def ensure_table(cls, conn):
        """Ensure the table exists in the database."""
        cls.__table__.create(bind=conn, checkfirst=True)

    @classmethod
    def validate_table(cls, conn):
        """
        Validate that the table exists and matches the declared columns.
        Raises RuntimeError if the table is missing or columns are mismatched.
        """
        here = "valid_tbl"
        namespace = cls.namespace()
        tablename = cls.tablename()
        table_columns = cls.__table__.columns

        with log_block(here, f"Table '{cls.table()}' validation"):
            inspector = inspect(conn)

            # Check if the table exists
            tables = inspector.get_table_names(schema=namespace)
            if tablename not in tables:
                raise RuntimeError(f"Table '{tablename}' does not exist in schema '{namespace}'")

            errors = []
            actual_columns_info = {col["name"]: col for col in inspector.get_columns(tablename, schema=namespace)}
            for col_name, col_obj in table_columns.items():
                if col_name not in actual_columns_info:
                    errors.append(f"Column '{col_name}' missing")
                    continue

                actual_db_type: str = actual_columns_info[col_name]["type"].compile(dialect=conn.dialect)
                expected_type: str = col_obj.type.compile(dialect=conn.dialect)

                if actual_db_type != expected_type:
                    errors.append(
                        f"Column '{col_name}' type mismatch: expected {expected_type}, got {{actual_db_type}}"
                    )

                if actual_columns_info[col_name]["nullable"] != col_obj.nullable:
                    errors.append(
                        f"Column '{col_name}' nullability mismatch: expected {col_obj.nullable},"
                        f" got {actual_columns_info[col_name]['nullable']}"
                    )

            actual_columns = set(actual_columns_info.keys())
            declared_columns = set(table_columns.keys())

            extra_columns = actual_columns - declared_columns
            for col_name in extra_columns:
                errors.append(f"Extra column '{col_name}' exists in DB but not in model")

            if errors:
                err_msg = f"Table '{cls.table()}' does not match model declaration:\n" + "\n".join(errors)
                log_e(mod, err_msg)
                raise RuntimeError(err_msg)
