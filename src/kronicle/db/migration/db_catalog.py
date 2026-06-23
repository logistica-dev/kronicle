# kronicle/db/migration/db_catalog.py
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from json import dumps

from sqlalchemy import inspect
from sqlalchemy.schema import Table

# ==================================================================================================
# Column Catalog
# ==================================================================================================


@dataclass(frozen=True)
class ColumnCatalog:
    name: str
    type: str
    nullable: bool | None
    default: str | None
    primary_key: bool

    def as_tuple(self) -> tuple:
        return (self.name, self.type, self.nullable)


# ==================================================================================================
# Table Catalog
# ==================================================================================================


@dataclass(frozen=True)
class TableCatalog:
    name: str
    columns: tuple[ColumnCatalog, ...]

    def as_tuple(self) -> tuple:
        return (self.name, tuple(c.as_tuple() for c in self.columns))


# ==================================================================================================
# Database Catalog
# ==================================================================================================


@dataclass(frozen=True)
class DatabaseCatalog:
    namespace: str
    tables: tuple[TableCatalog, ...]

    def as_tuple(self) -> tuple:
        return (self.namespace, tuple(t.as_tuple() for t in self.tables))

    def compute_hash(self) -> str:

        raw = dumps(self.as_tuple(), sort_keys=True)
        return sha256(raw.encode()).hexdigest()


# ==================================================================================================
# Builder
# ==================================================================================================


class DatabaseCatalogBuilder:
    """
    Builds a normalized snapshot of DB state or SQLAlchemy metadata.
    """

    def __init__(self, connection):
        self.connection = connection
        self.inspector = inspect(connection)

    # ------------------------------------------------------------------
    # DB → catalog
    # ------------------------------------------------------------------

    def from_database(self, namespace: str) -> DatabaseCatalog:
        tables: list[TableCatalog] = []

        for table_name in sorted(self.inspector.get_table_names(schema=namespace)):
            columns = self.inspector.get_columns(table_name, schema=namespace)
            pk_cols = set(self.inspector.get_pk_constraint(table_name, schema=namespace).get("constrained_columns", []))

            col_catalogs = sorted(
                (
                    ColumnCatalog(
                        name=c["name"],
                        type=self._normalize_type(str(c["type"])),
                        nullable=c["nullable"],
                        default=None,
                        primary_key=c["name"] in pk_cols,
                    )
                    for c in columns
                ),
                key=lambda cc: cc.name,
            )

            tables.append(TableCatalog(name=table_name, columns=tuple(col_catalogs)))

        return DatabaseCatalog(namespace=namespace, tables=tuple(tables))

    # ------------------------------------------------------------------
    # SQLAlchemy → catalog
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_type(t: str) -> str:
        return t.replace("DATETIME", "TIMESTAMP")

    @classmethod
    def from_metadata(cls, tables: dict[str, Table]) -> DatabaseCatalog:
        grouped: dict[str, list[TableCatalog]] = {}

        for table in tables.values():
            if table.schema is None:
                raise RuntimeError(f"Unscoped table detected: {table.name}")
            schema = table.schema

            col_catalogs = sorted(
                (
                    ColumnCatalog(
                        name=c.name,
                        type=cls._normalize_type(str(c.type)),
                        nullable=c.nullable,
                        default=None,
                        primary_key=c.primary_key,
                    )
                    for c in table.columns
                ),
                key=lambda cc: cc.name,
            )
            grouped.setdefault(schema, []).append(TableCatalog(name=table.name, columns=tuple(col_catalogs)))

        namespace = next(iter(grouped.keys()))
        tables_in_order = sorted(grouped[namespace], key=lambda t: t.name)
        return DatabaseCatalog(namespace=namespace, tables=tuple(tables_in_order))
