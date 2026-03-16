from pydantic import ValidationError
from pytest import raises

from kronicle.schemas.filters.request_filter import DEFAULT_LIMIT, MAX_LIMIT, MAX_OFFSET, RequestFilter
from kronicle.types.iso_datetime import IsoDateTime


# -----------------------------------------------------------------------------
# Defaults
# -----------------------------------------------------------------------------
def test_defaults():
    f = RequestFilter()

    assert f.from_date is None
    assert f.to_date is None
    assert f.limit == DEFAULT_LIMIT
    assert f.offset is None


# -----------------------------------------------------------------------------
# Limit / offset validation
# -----------------------------------------------------------------------------
def test_limit_bounds():
    assert RequestFilter(limit=MAX_LIMIT + 1).limit == MAX_LIMIT
    assert RequestFilter(limit=-1).limit == DEFAULT_LIMIT


def test_offset_bounds():
    assert RequestFilter(offset=MAX_OFFSET + 1).offset == MAX_OFFSET
    assert RequestFilter(offset=-1).offset == 0


def test_limit_and_offset_valid():
    f = RequestFilter(limit=100, offset=10)
    assert f.limit == 100
    assert f.offset == 10


# -----------------------------------------------------------------------------
# Date validation
# -----------------------------------------------------------------------------
def test_to_date_before_from_date_is_not_ignored():
    from_date = IsoDateTime("2024-01-10T00:00:00Z")
    to_date = IsoDateTime("2024-01-01T00:00:00Z")

    with raises(ValidationError):
        RequestFilter(from_date=from_date, to_date=to_date)


def test_valid_date_range():
    from_date = IsoDateTime("2024-01-01T00:00:00Z")
    to_date = IsoDateTime("2024-01-10T00:00:00Z")

    f = RequestFilter(from_date=from_date, to_date=to_date)

    assert f.from_date == from_date
    assert f.to_date == to_date


# -----------------------------------------------------------------------------
# SQL generation
# -----------------------------------------------------------------------------
def test_sql_no_filters():
    f = RequestFilter()
    sql, params = f.to_sql_clauses()

    assert "WHERE" not in sql
    assert f"LIMIT {DEFAULT_LIMIT}" in sql
    assert "OFFSET 0" not in sql  # offset is 0 → omitted
    assert params == []


def test_sql_with_from_date():
    from_date = IsoDateTime("2024-01-01T00:00:00Z")
    f = RequestFilter(from_date=from_date)

    sql, params = f.to_sql_clauses()

    assert "WHERE time >= $1" in sql
    assert params == [from_date]


def test_sql_with_full_range_and_pagination():
    from_date = IsoDateTime("2024-01-01T00:00:00Z")
    to_date = IsoDateTime("2024-01-10T00:00:00Z")

    f = RequestFilter(from_date=from_date, to_date=to_date, limit=100, offset=20)
    sql, params = f.to_sql_clauses()

    assert "WHERE time >= $1 AND time <= $2" in sql
    assert "LIMIT 100" in sql
    assert "OFFSET 20" in sql
    assert params == [from_date, to_date]


def test_sql_start_index_offset():
    from_date = IsoDateTime("2024-01-01T00:00:00Z")
    f = RequestFilter(from_date=from_date)

    sql, params = f.to_sql_clauses(start_idx=5)

    assert "time >= $5" in sql
    assert params == [from_date]


def test_sql_max_limit():
    limit = 600
    f = RequestFilter(limit=limit)

    sql, params = f.to_sql_clauses(start_idx=5)
    assert f"LIMIT {MAX_LIMIT}" in sql
    assert params == []


if __name__ == "__main__":  # pragma: no cover
    print(RequestFilter(limit=100).to_sql_clauses())
    print(RequestFilter(limit=600, offset=-1).to_sql_clauses())
    print(RequestFilter(limit=-1, from_date=IsoDateTime.now()).to_sql_clauses())
