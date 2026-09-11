# tests/unit/db/migration/engine/test_db_catalog.py
from unittest.mock import MagicMock, patch

from sqlalchemy import Column, ForeignKey, MetaData, String, Table

from kronicle.db.migration.engine.db_catalog import (
    ColumnCatalog,
    DatabaseCatalog,
    DatabaseCatalogBuilder,
    ForeignKeyCatalog,
    TableCatalog,
)


def _column(name, type_str="INTEGER", nullable=True, primary_key=False):
    return ColumnCatalog(name=name, type=type_str, nullable=nullable, default=None, primary_key=primary_key)


def _table(name="t1", columns=(), foreign_keys=()):
    return TableCatalog(name=name, columns=columns, foreign_keys=foreign_keys)


class TestColumnCatalog:
    def test_as_tuple(self):
        col = _column("a", primary_key=True)
        assert col.as_tuple() == ("a", "INTEGER", True, True)


class TestForeignKeyCatalog:
    def test_as_tuple(self):
        fk = ForeignKeyCatalog(
            name="fk",
            local_columns=("a",),
            referred_schema="core",
            referred_table="t2",
            referred_columns=("id",),
            ondelete="CASCADE",
            onupdate=None,
        )
        assert fk.as_tuple() == ("fk", ("a",), "core", "t2", ("id",), "CASCADE", None)


class TestTableCatalog:
    def test_as_tuple(self):
        t = _table("t1", columns=(_column("a"),))
        assert t.as_tuple()[0] == "t1"
        assert t.as_tuple()[1] == (("a", "INTEGER", True, False),)


class TestDatabaseCatalog:
    def test_as_tuple(self):
        catalog = DatabaseCatalog(namespace="core", tables=(_table(),))
        assert catalog.as_tuple() == ("core", (("t1", (()), (())),))

    def test_compute_hash_is_deterministic(self):
        catalog = DatabaseCatalog(namespace="core", tables=())
        assert catalog.compute_hash() == catalog.compute_hash()

    def test_compute_hash_differs_for_different_content(self):
        a = DatabaseCatalog(namespace="core", tables=())
        b = DatabaseCatalog(namespace="rbac", tables=())
        assert a.compute_hash() != b.compute_hash()

    def test_compute_hash_reflects_table_order(self):
        t1 = _table("a", columns=(_column("x"),))
        t2 = _table("b")
        first = DatabaseCatalog(namespace="core", tables=(t1, t2)).compute_hash()
        swapped = DatabaseCatalog(namespace="core", tables=(t2, t1)).compute_hash()
        assert first != swapped


class TestFromDatabase:
    def test_builds_catalog_from_inspector(self):
        inspector = MagicMock()
        inspector.get_table_names.return_value = ["channels", "zones"]
        inspector.get_columns.return_value = [
            {"name": "id", "type": "UUID", "nullable": False},
            {"name": "name", "type": "VARCHAR(64)", "nullable": True},
        ]
        inspector.get_pk_constraint.return_value = {"constrained_columns": ["id"]}
        inspector.get_foreign_keys.return_value = [
            {
                "name": "fk_zone",
                "constrained_columns": ["zone_id"],
                "referred_schema": "core",
                "referred_table": "zones",
                "referred_columns": ["id"],
                "options": {"ondelete": "CASCADE"},
            }
        ]

        with patch("kronicle.db.migration.engine.db_catalog.inspect", return_value=inspector):
            builder = DatabaseCatalogBuilder(MagicMock())
            catalog = builder.from_database("core")

        assert catalog.namespace == "core"
        assert [t.name for t in catalog.tables] == ["channels", "zones"]
        channels = catalog.tables[0]
        assert channels.columns[0].primary_key is True
        assert channels.columns[1].primary_key is False


class TestNormalizeType:
    def test_normalizes_datetime(self):
        assert DatabaseCatalogBuilder._normalize_type("DATETIME") == "TIMESTAMP"

    def test_leaves_other_types_untouched(self):
        assert DatabaseCatalogBuilder._normalize_type("UUID") == "UUID"


class TestFromMetadata:
    def test_sorts_tables_and_columns(self):
        metadata = MetaData()
        Table(
            "zones",
            metadata,
            Column("id", String(36), primary_key=True),
            Column("name", String(64), nullable=False),
            schema="core",
        )
        Table(
            "channels",
            metadata,
            Column("id", String(36), primary_key=True),
            Column("zone_id", ForeignKey("core.zones.id"), nullable=False),
            schema="core",
        )
        catalog = DatabaseCatalogBuilder.from_metadata(dict(metadata.tables))

        assert catalog.namespace == "core"
        assert [t.name for t in catalog.tables] == ["channels", "zones"]
        channels = catalog.tables[0]
        assert [c.name for c in channels.columns] == ["id", "zone_id"]
        assert len(channels.foreign_keys) == 1
        assert channels.foreign_keys[0].referred_table == "zones"
        assert channels.foreign_keys[0].local_columns == ("zone_id",)

    def test_raises_for_unscoped_table(self):
        metadata = MetaData()
        Table("naked", metadata, Column("id", String(36), primary_key=True))
        try:
            DatabaseCatalogBuilder.from_metadata(dict(metadata.tables))
        except RuntimeError as e:
            assert "Unscoped table" in str(e)
        else:
            raise AssertionError("Expected RuntimeError")
