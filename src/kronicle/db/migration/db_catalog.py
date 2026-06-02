# kronicle/db/migration/database_catalog.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

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

    def as_tuple(self) -> Tuple:
        return (self.name, self.type, self.nullable, self.default, self.primary_key)


# ==================================================================================================
# Table Catalog
# ==================================================================================================


@dataclass(frozen=True)
class TableCatalog:
    name: str
    columns: Tuple[ColumnCatalog, ...]

    def as_tuple(self) -> Tuple:
        return (self.name, tuple(c.as_tuple() for c in self.columns))


# ==================================================================================================
# Database Catalog
# ==================================================================================================


@dataclass(frozen=True)
class DatabaseCatalog:
    namespace: str
    tables: Tuple[TableCatalog, ...]

    def as_tuple(self) -> Tuple:
        return (self.namespace, tuple(t.as_tuple() for t in self.tables))

    def compute_hash(self) -> str:
        import hashlib
        import json

        raw = json.dumps(self.as_tuple(), sort_keys=True)
        return hashlib.sha256(raw.encode()).hexdigest()


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
        tables: List[TableCatalog] = []

        for table_name in self.inspector.get_table_names(schema=namespace):
            columns = self.inspector.get_columns(table_name, schema=namespace)

            col_catalogs = [
                ColumnCatalog(
                    name=c["name"],
                    type=str(c["type"]),
                    nullable=c["nullable"],
                    default=str(c.get("default")) if c.get("default") else None,
                    primary_key=c.get("primary_key", False),
                )
                for c in columns
            ]

            tables.append(TableCatalog(name=table_name, columns=tuple(col_catalogs)))

        return DatabaseCatalog(namespace=namespace, tables=tuple(tables))

    # ------------------------------------------------------------------
    # SQLAlchemy → catalog
    # ------------------------------------------------------------------

    @classmethod
    def from_metadata(cls, tables: Dict[str, Table]) -> DatabaseCatalog:
        grouped: Dict[str, List[TableCatalog]] = {}

        for table in tables.values():
            if table.schema is None:
                raise RuntimeError(f"Unscoped table detected: {table.name}")
            schema = table.schema

            col_catalogs = [
                ColumnCatalog(
                    name=c.name,
                    type=str(c.type),
                    nullable=c.nullable,
                    default=str(c.default) if c.default is not None else None,
                    primary_key=c.primary_key,
                )
                for c in table.columns
            ]
            grouped.setdefault(schema, []).append(TableCatalog(name=table.name, columns=tuple(col_catalogs)))

        namespace = next(iter(grouped.keys()))
        return DatabaseCatalog(namespace=namespace, tables=tuple(grouped[namespace]))
