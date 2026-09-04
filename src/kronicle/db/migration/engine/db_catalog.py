# kronicle/db/migration/db_catalog.py
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from json import dumps

from sqlalchemy import inspect
from sqlalchemy.schema import ForeignKeyConstraint, Table

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
        return (self.name, self.type, self.nullable, self.primary_key)


@dataclass(frozen=True)
class ForeignKeyCatalog:
    name: str | None
    local_columns: tuple[str, ...]
    referred_schema: str | None
    referred_table: str | None
    referred_columns: tuple[str, ...]
    ondelete: str | None
    onupdate: str | None

    def as_tuple(self) -> tuple:
        return (
            self.name,
            self.local_columns,
            self.referred_schema,
            self.referred_table,
            self.referred_columns,
            self.ondelete,
            self.onupdate,
        )


# ==================================================================================================
# Table Catalog
# ==================================================================================================


@dataclass(frozen=True)
class TableCatalog:
    name: str
    columns: tuple[ColumnCatalog, ...]
    foreign_keys: tuple[ForeignKeyCatalog, ...] = ()

    def as_tuple(self) -> tuple:
        return (self.name, tuple(c.as_tuple() for c in self.columns), tuple(f.as_tuple() for f in self.foreign_keys))


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
            fks = self.inspector.get_foreign_keys(table_name, schema=namespace)

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

            fk_catalogs = tuple(
                sorted(
                    (
                        ForeignKeyCatalog(
                            name=fk.get("name"),
                            local_columns=tuple(fk.get("constrained_columns", ())),
                            referred_schema=fk.get("referred_schema"),
                            referred_table=fk.get("referred_table"),
                            referred_columns=tuple(fk.get("referred_columns", ())),
                            ondelete=(fk.get("options") or {}).get("ondelete"),
                            onupdate=(fk.get("options") or {}).get("onupdate"),
                        )
                        for fk in fks
                    ),
                    key=lambda f: (
                        f.local_columns,
                        f.referred_schema or "",
                        f.referred_table or "",
                        f.referred_columns,
                    ),
                )
            )

            tables.append(TableCatalog(name=table_name, columns=tuple(col_catalogs), foreign_keys=fk_catalogs))

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

            fk_catalogs = tuple(
                sorted(
                    (
                        ForeignKeyCatalog(
                            name=cons.name if isinstance(cons.name, str) else None,
                            local_columns=tuple(col.name for col in cons.columns),
                            referred_schema=(
                                elements[0].column.table.schema
                                if (elements := list(cons.elements)) and elements[0].column is not None
                                else None
                            ),
                            referred_table=(
                                elements[0].column.table.name
                                if (elements := list(cons.elements)) and elements[0].column is not None
                                else None
                            ),
                            referred_columns=tuple(elem.column.name for elem in cons.elements),
                            ondelete=cons.ondelete,
                            onupdate=cons.onupdate,
                        )
                        for cons in table.constraints
                        if isinstance(cons, ForeignKeyConstraint)
                    ),
                    key=lambda f: (
                        f.local_columns,
                        f.referred_schema or "",
                        f.referred_table or "",
                        f.referred_columns,
                    ),
                )
            )

            grouped.setdefault(schema, []).append(
                TableCatalog(name=table.name, columns=tuple(col_catalogs), foreign_keys=fk_catalogs)
            )

        namespace = next(iter(grouped.keys()))
        tables_in_order = sorted(grouped[namespace], key=lambda t: t.name)
        return DatabaseCatalog(namespace=namespace, tables=tuple(tables_in_order))
