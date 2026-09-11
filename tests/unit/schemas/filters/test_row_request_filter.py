# tests/unit/schemas/filters/test_row_request_filter.py

from kronicle.schemas.filters.row_query_filter import RowQueryFilter
from kronicle.schemas.filters.row_request_filter import RowRequestFilter


def test_feedback_property_returns_op_feedback():
    rf = RowRequestFilter()
    assert rf.feedback is not None


def test_copy_with_feedback_copies_feedback():
    rf = RowRequestFilter(strict=True)
    rf._feedback.add_detail(message="test", field="query")
    copied = rf.copy_with_feedback()
    assert copied.feedback is rf.feedback
    assert copied.feedback.details == rf.feedback.details
    assert copied.strict is True


def test_copy_with_feedback_preserves_fields():
    rf = RowRequestFilter(limit=50, sort=["col1", "-col2"], strict=False)
    copied = rf.copy_with_feedback()
    assert copied.limit == 50
    assert copied.sort == ["col1", "-col2"]
    assert copied.strict is False


def test_from_query_converts_query_filter():
    qf = RowQueryFilter(limit=42, sort="col1", columns="col1,col2")
    rr = RowRequestFilter.from_query(qf)
    assert rr.limit == 42
    assert rr.sort == ["col1"]


def test_from_query_copies_feedback():
    qf = RowQueryFilter()
    qf._feedback.add_detail(message="qf_feedback", field="query")
    rr = RowRequestFilter.from_query(qf)
    assert [d.json for d in rr.feedback.details] == [
        {"message": "qf_feedback", "field": "query", "subfield": None, "extra": None}
    ]


def test_from_query_with_no_columns():
    qf = RowQueryFilter()
    rr = RowRequestFilter.from_query(qf)
    assert rr.columns is None


def test_feedback_starts_empty():
    rf = RowRequestFilter()
    assert rf.feedback.details == []
