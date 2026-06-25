# tests/unit/db/base/test_kronicle_base.py
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from kronicle.db.base.kronicle_base import KronicleBase


class ConcreteTable(KronicleBase):
    __tablename__ = "test_table"

    extra_field: Mapped[str] = mapped_column(String, nullable=True)

    @classmethod
    def namespace(cls):
        return "test_schema"


@pytest.fixture
def mock_conn():
    return MagicMock()


class TestNamespace:
    def test_raises_on_base_class(self):
        with pytest.raises(NotImplementedError, match="Method namespace"):
            KronicleBase.namespace()

    def test_returns_from_subclass(self):
        assert ConcreteTable.namespace() == "test_schema"


class TestTablename:
    def test_raises_on_base_class(self):
        with pytest.raises(NotImplementedError, match="abstract class"):
            KronicleBase.tablename()

    def test_returns_from_subclass(self):
        assert ConcreteTable.tablename() == "test_table"


class TestTable:
    def test_returns_qualified_table_name(self):
        assert ConcreteTable.table() == "test_schema.test_table"


class TestEnsureTable:
    def test_calls_create_on_connection(self, mock_conn):
        with patch.object(ConcreteTable.__table__, "create") as mock_create:
            ConcreteTable.ensure_table(mock_conn)
            mock_create.assert_called_once_with(bind=mock_conn, checkfirst=True)


class TestValidateTable:
    def test_table_valid(self, mock_conn):
        inspector = MagicMock()
        inspector.get_table_names.return_value = ["test_table"]
        inspector.get_columns.return_value = [
            {"name": "id", "type": MagicMock(), "nullable": False},
            {"name": "created_at", "type": MagicMock(), "nullable": False},
            {"name": "updated_at", "type": MagicMock(), "nullable": False},
            {"name": "details", "type": MagicMock(), "nullable": False},
            {"name": "extra_field", "type": MagicMock(), "nullable": True},
        ]

        with patch("kronicle.db.base.kronicle_base.inspect", return_value=inspector):
            result = ConcreteTable.validate_table(mock_conn)

        assert result is None

    def test_table_missing_raises(self, mock_conn):
        inspector = MagicMock()
        inspector.get_table_names.return_value = ["other_table"]

        with patch("kronicle.db.base.kronicle_base.inspect", return_value=inspector):
            with pytest.raises(RuntimeError, match="does not exist"):
                ConcreteTable.validate_table(mock_conn)

    def test_column_missing_logs_error(self, mock_conn):
        inspector = MagicMock()
        inspector.get_table_names.return_value = ["test_table"]
        inspector.get_columns.return_value = [
            {"name": "id", "type": MagicMock(), "nullable": False},
        ]

        with (
            patch("kronicle.db.base.kronicle_base.inspect", return_value=inspector),
            patch("kronicle.db.base.kronicle_base.log_e") as mock_log_e,
        ):
            result = ConcreteTable.validate_table(mock_conn)

        assert result is None
        mock_log_e.assert_called_once()
        assert "does not match model" in mock_log_e.call_args[0][1]

    def test_type_mismatch_logs_error(self, mock_conn):
        inspector = MagicMock()
        inspector.get_table_names.return_value = ["test_table"]

        int_type = MagicMock()
        int_type.compile.return_value = "INTEGER"
        varchar_type = MagicMock()
        varchar_type.compile.return_value = "VARCHAR(255)"
        uuid_type = MagicMock()
        uuid_type.compile.return_value = "UUID"
        dttz_type = MagicMock()
        dttz_type.compile.return_value = "DATETIME"
        jsonb_type = MagicMock()
        jsonb_type.compile.return_value = "JSONB"

        inspector.get_columns.return_value = [
            {"name": "id", "type": int_type, "nullable": False},
            {"name": "created_at", "type": dttz_type, "nullable": False},
            {"name": "updated_at", "type": dttz_type, "nullable": False},
            {"name": "details", "type": jsonb_type, "nullable": False},
            {"name": "extra_field", "type": varchar_type, "nullable": True},
        ]

        id_col = ConcreteTable.__table__.columns["id"]
        with patch.object(id_col.type, "compile", return_value="UUID"):
            with (
                patch("kronicle.db.base.kronicle_base.inspect", return_value=inspector),
                patch("kronicle.db.base.kronicle_base.log_e") as mock_log_e,
            ):
                result = ConcreteTable.validate_table(mock_conn)

        assert result is None
        mock_log_e.assert_called_once()

    def test_nullability_mismatch_logs_error(self, mock_conn):
        inspector = MagicMock()
        inspector.get_table_names.return_value = ["test_table"]

        def make_col(nullable):
            return {"name": "n", "type": MagicMock(), "nullable": nullable}

        inspector.get_columns.return_value = [
            {"name": "id", "type": MagicMock(), "nullable": True},
            {"name": "created_at", "type": MagicMock(), "nullable": False},
            {"name": "updated_at", "type": MagicMock(), "nullable": False},
            {"name": "details", "type": MagicMock(), "nullable": False},
            {"name": "extra_field", "type": MagicMock(), "nullable": False},
        ]

        with (
            patch("kronicle.db.base.kronicle_base.inspect", return_value=inspector),
            patch("kronicle.db.base.kronicle_base.log_e") as mock_log_e,
        ):
            result = ConcreteTable.validate_table(mock_conn)

        assert result is None
        assert mock_log_e.called

    def test_extra_columns_logged_as_warning(self, mock_conn):
        inspector = MagicMock()
        inspector.get_table_names.return_value = ["test_table"]
        inspector.get_columns.return_value = [
            {"name": "id", "type": MagicMock(), "nullable": False},
            {"name": "created_at", "type": MagicMock(), "nullable": False},
            {"name": "updated_at", "type": MagicMock(), "nullable": False},
            {"name": "details", "type": MagicMock(), "nullable": False},
            {"name": "extra_field", "type": MagicMock(), "nullable": True},
            {"name": "ghost_column", "type": MagicMock(), "nullable": True},
        ]

        with (
            patch("kronicle.db.base.kronicle_base.inspect", return_value=inspector),
            patch("kronicle.db.base.kronicle_base.log_w") as mock_log_w,
        ):
            result = ConcreteTable.validate_table(mock_conn)

        assert result is None
        extra_calls = [c for c in mock_log_w.call_args_list if "extra column" in str(c)]
        assert len(extra_calls) == 1
        assert "ghost_column" in str(extra_calls[0])
