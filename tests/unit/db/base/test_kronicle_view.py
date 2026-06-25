# tests/unit/db/base/test_kronicle_view.py
import pytest
from sqlalchemy import Column, Integer

from kronicle.db.base.kronicle_view import KronicleView


class ConcreteView(KronicleView):
    __tablename__ = "test_view_table"
    id = Column(Integer, primary_key=True)

    @classmethod
    def namespace(cls):
        return "test_schema"


class TestNamespace:
    def test_raises_on_base_class(self):
        with pytest.raises(NotImplementedError, match="Method namespace"):
            KronicleView.namespace()

    def test_returns_from_subclass(self):
        assert ConcreteView.namespace() == "test_schema"


class TestTablename:
    def test_raises_on_base_class(self):
        with pytest.raises(NotImplementedError, match="abstract class"):
            KronicleView.tablename()

    def test_returns_from_subclass(self):
        assert ConcreteView.tablename() == "test_view_table"


class TestTable:
    def test_returns_qualified_name(self):
        assert ConcreteView.table() == "test_schema.test_view_table"


class TestCreateViewSql:
    def test_raises_on_base(self):
        with pytest.raises(NotImplementedError):
            KronicleView.create_view_sql()

    def test_raises_on_subclass_without_override(self):
        with pytest.raises(NotImplementedError):
            ConcreteView.create_view_sql()


class TestIsView:
    def test_marker_is_true(self):
        assert ConcreteView.is_view is True
