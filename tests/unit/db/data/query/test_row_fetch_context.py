# tests/unit/db/data/query/test_row_fetch_context.py
from __future__ import annotations

from unittest.mock import patch

import pytest

from kronicle.db.data.models.schema_types import SchemaType
from kronicle.db.data.query.col_filters import (
    AnyFilter,
    ExactFilter,
    HasFilter,
    MaxFilter,
    MinFilter,
)
from kronicle.db.data.query.row_fetch_context import RowFetchContext
from kronicle.errors.error_types import BadRequestError
from kronicle.schemas.filters.row_request_filter import RowRequestFilter
from kronicle.schemas.payload.op_feedback import OpFeedback


@pytest.fixture
def column_types():
    return {
        "name": SchemaType("string"),
        "age": SchemaType("int"),
        "score": SchemaType("float"),
        "active": SchemaType("bool"),
        "received_at": SchemaType("datetime"),
        "tags": SchemaType("list"),
        "metadata": SchemaType("dict"),
    }


@pytest.fixture
def mock_log_block():
    with patch("kronicle.db.data.query.row_fetch_context.log_block") as m:
        yield m


@pytest.fixture
def mock_log_w():
    with patch("kronicle.db.data.query.row_fetch_context.log_w") as m:
        yield m


@pytest.fixture
def mock_log_d():
    with patch("kronicle.db.data.query.row_fetch_context.log_d") as m:
        yield m


class TestFiltersProperty:
    def test_creates_default_when_none(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        with patch.object(RowFetchContext, "_resolve_filters"), patch.object(RowFetchContext, "_resolve_sort"):
            ctx = RowFetchContext(column_types=column_types)
        assert ctx.in_filters is None
        f = ctx.filters
        assert isinstance(f, RowRequestFilter)
        assert ctx.in_filters is not None
        assert f is ctx.in_filters

    def test_returns_in_filters(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        in_filters = RowRequestFilter()
        with patch.object(RowFetchContext, "_resolve_filters"), patch.object(RowFetchContext, "_resolve_sort"):
            ctx = RowFetchContext(column_types=column_types, in_filters=in_filters)
        assert ctx.filters is in_filters

    def test_idempotent(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        with patch.object(RowFetchContext, "_resolve_filters"), patch.object(RowFetchContext, "_resolve_sort"):
            ctx = RowFetchContext(column_types=column_types)
        f1 = ctx.filters
        f2 = ctx.filters
        assert f1 is f2


class TestProperties:
    def test_limit(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        ctx = RowFetchContext(column_types=column_types, in_filters=RowRequestFilter(limit=25))
        assert ctx.limit == 25

    def test_offset(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        ctx = RowFetchContext(column_types=column_types, in_filters=RowRequestFilter(offset=10))
        assert ctx.offset == 10

    def test_limit_default(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        ctx = RowFetchContext(column_types=column_types, in_filters=RowRequestFilter())
        assert ctx.limit == 100

    def test_offset_default(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        ctx = RowFetchContext(column_types=column_types, in_filters=RowRequestFilter())
        assert ctx.offset is None

    def test_feedback(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        ctx = RowFetchContext(column_types=column_types, in_filters=RowRequestFilter())
        assert isinstance(ctx.feedback, OpFeedback)

    def test_feedback_empty(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        ctx = RowFetchContext(column_types=column_types, in_filters=RowRequestFilter())
        assert not ctx.feedback.has_details


class TestResolveFilters:
    def test_adds_all_filter_types(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        in_filters = RowRequestFilter(
            col={"name": "Alice"},
            min={"age": "25"},
            max={"score": "100.5"},
            any={"name": ["Alice", "Bob"]},
            has={"tags": ["admin"]},
        )
        ctx = RowFetchContext(column_types=column_types, in_filters=in_filters)
        assert len(ctx._filters) == 5
        assert isinstance(ctx._filters[0], ExactFilter)
        assert isinstance(ctx._filters[1], MinFilter)
        assert isinstance(ctx._filters[2], MaxFilter)
        assert isinstance(ctx._filters[3], AnyFilter)
        assert isinstance(ctx._filters[4], HasFilter)

    def test_empty_dicts_add_no_filters(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        ctx = RowFetchContext(column_types=column_types, in_filters=RowRequestFilter())
        assert len(ctx._filters) == 0

    def test_unknown_column_adds_feedback(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        in_filters = RowRequestFilter(col={"unknown_col": "value"})
        ctx = RowFetchContext(column_types=column_types, in_filters=in_filters)
        assert ctx.feedback.has_details
        assert len(ctx.feedback.details) == 1

    def test_dict_without_subkeys_adds_feedback(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        in_filters = RowRequestFilter(col={"metadata": "value"})
        ctx = RowFetchContext(column_types=column_types, in_filters=in_filters)
        assert ctx.feedback.has_details

    def test_subkeys_on_non_dict_adds_feedback(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        in_filters = RowRequestFilter(col={"name.first": "Alice"})
        ctx = RowFetchContext(column_types=column_types, in_filters=in_filters)
        assert ctx.feedback.has_details

    def test_subkeys_valid(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        in_filters = RowRequestFilter(col={"metadata.temp": "25"})
        ctx = RowFetchContext(column_types=column_types, in_filters=in_filters)
        assert len(ctx._filters) == 1
        assert ctx._filters[0].col.subkeys == ["temp"]

    def test_range_on_list_adds_feedback(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        in_filters = RowRequestFilter(min={"tags": "admin"})
        ctx = RowFetchContext(column_types=column_types, in_filters=in_filters)
        assert ctx.feedback.has_details

    def test_any_on_list_adds_feedback(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        in_filters = RowRequestFilter(any={"tags": ["admin"]})
        ctx = RowFetchContext(column_types=column_types, in_filters=in_filters)
        assert ctx.feedback.has_details


class TestResolveSort:
    def test_default_when_none(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        ctx = RowFetchContext(column_types=column_types, in_filters=RowRequestFilter(sort=None))
        assert len(ctx._sort) == 1
        assert ctx._sort[0].col.col_name == "received_at"
        assert ctx._sort[0].desc is True

    def test_default_when_empty_list(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        ctx = RowFetchContext(column_types=column_types, in_filters=RowRequestFilter(sort=[]))
        assert len(ctx._sort) == 1
        assert ctx._sort[0].col.col_name == "received_at"

    def test_valid_sort_columns(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        ctx = RowFetchContext(column_types=column_types, in_filters=RowRequestFilter(sort=["name", "-age"]))
        assert len(ctx._sort) == 2
        assert ctx._sort[0].col.col_name == "name"
        assert ctx._sort[0].desc is False
        assert ctx._sort[1].col.col_name == "age"
        assert ctx._sort[1].desc is True

    def test_unknown_column_adds_feedback(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        ctx = RowFetchContext(column_types=column_types, in_filters=RowRequestFilter(sort=["bad_column"]))
        assert ctx.feedback.has_details

    def test_collection_column_adds_feedback(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        ctx = RowFetchContext(column_types=column_types, in_filters=RowRequestFilter(sort=["tags"]))
        assert ctx.feedback.has_details


class TestToSql:
    def test_where_clause(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        ctx = RowFetchContext(column_types=column_types, in_filters=RowRequestFilter(col={"name": "Alice"}))
        sql, params = ctx.to_sql()
        assert "WHERE" in sql
        assert "name =" in sql
        assert params == ["Alice"]

    def test_order_by(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        ctx = RowFetchContext(column_types=column_types, in_filters=RowRequestFilter(sort=["name"]))
        sql, params = ctx.to_sql()
        assert "ORDER BY" in sql
        assert "name ASC" in sql

    def test_limit(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        ctx = RowFetchContext(column_types=column_types, in_filters=RowRequestFilter(limit=10))
        sql, params = ctx.to_sql()
        assert "LIMIT 10" in sql

    def test_offset(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        ctx = RowFetchContext(column_types=column_types, in_filters=RowRequestFilter(offset=20))
        sql, params = ctx.to_sql()
        assert "OFFSET 20" in sql

    def test_all_components(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        in_filters = RowRequestFilter(
            col={"name": "Alice"},
            min={"age": "18"},
            sort=["-received_at"],
            limit=50,
            offset=10,
        )
        ctx = RowFetchContext(column_types=column_types, in_filters=in_filters)
        sql, params = ctx.to_sql()
        assert sql.startswith("WHERE")
        assert "AND" in sql
        assert "ORDER BY" in sql
        assert "LIMIT 50" in sql
        assert "OFFSET 10" in sql
        assert 18 in params
        assert "Alice" in params

    def test_no_filters_has_default_sort(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        ctx = RowFetchContext(column_types=column_types, in_filters=RowRequestFilter())
        sql, params = ctx.to_sql()
        assert "WHERE" not in sql
        assert "ORDER BY" in sql
        assert "received_at DESC" in sql
        assert params == []


class TestStrictMode:
    def test_strict_raises_bad_request(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        in_filters = RowRequestFilter(col={"unknown_col": "value"}, strict=True)
        with pytest.raises(BadRequestError):
            RowFetchContext(column_types=column_types, in_filters=in_filters)

    def test_non_strict_does_not_raise(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        in_filters = RowRequestFilter(col={"unknown_col": "value"}, strict=False)
        ctx = RowFetchContext(column_types=column_types, in_filters=in_filters)
        assert ctx.feedback.has_details


class TestModelPostInit:
    def test_resolves_filters(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        in_filters = RowRequestFilter(col={"name": "Alice"})
        ctx = RowFetchContext(column_types=column_types, in_filters=in_filters)
        assert len(ctx._filters) == 1

    def test_resolves_sort(self, column_types, mock_log_block, mock_log_d, mock_log_w):
        in_filters = RowRequestFilter(sort=["-received_at"])
        ctx = RowFetchContext(column_types=column_types, in_filters=in_filters)
        assert len(ctx._sort) == 1
