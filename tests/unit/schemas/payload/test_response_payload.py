# tests/unit/schemas/payload/test_response_payload.py
from uuid import uuid4

import pytest

from kronicle.db.data.models.channel_schema import ChannelSchema
from kronicle.schemas.payload.response_payload import ResponsePayload
from kronicle.types.iso_datetime import IsoDateTime


# Reuse your fixture function inside a helper for the mock
def make_channel_schema():
    return ChannelSchema.from_user_json(
        {
            "channel_id": "uuid",
            "name": "str",
            "fields": "dict",
            "tags": "dict",
            "metadata": "dict",
        }
    )


@pytest.fixture
def channel_schema_fixture():
    # Minimal valid ChannelSchema for testing
    return make_channel_schema()


# --- Mock classes ---
class MockChannelSchema:
    def model_dump(self, flatten=False):
        return {"mock": "schema", "flatten": flatten}


class MockTimeseries:
    def __init__(self, rows=None):
        self._rows = rows or []
        self.op_feedback = self

    def to_user_rows(self, skip_received=True):
        return self._rows

    def json(self):
        return {"feedback": "ok"}


class MockChannelResourceMetadata:
    def __init__(self):
        self.channel_id = uuid4()
        self.channel_schema = make_channel_schema()
        self.name = "test_channel"
        self.user_metadata = {"meta": 1}
        self.tags = {"tag": "tag1"}  # simple string tag
        self.received_at = IsoDateTime()


class MockChannelResource:
    def __init__(self, rows=None, row_nb=None):
        self.metadata = MockChannelResourceMetadata()
        self.timeseries = MockTimeseries(rows)
        self.row_nb = row_nb if row_nb is not None else len(rows or [])


# --- Basic instantiation ---
def test_basic_instantiation(channel_schema_fixture):
    payload = ResponsePayload(channel_id=uuid4(), channel_schema=channel_schema_fixture)
    assert payload.channel_id
    assert payload.channel_schema.model_dump(flatten=True) == {
        "channel_id": "uuid",
        "name": "str",
        "fields": "dict",
        "tags": "dict",
        "metadata": "dict",
    }
    # Optional fields should default to None or empty dict
    assert payload.metadata is None
    assert payload.tags is None
    assert payload.op_details == {}


# --- from_channel_resource factory ---
def test_from_channel_resource_basic():
    rows = [{"a": 1}, {"a": 2}]
    resource = MockChannelResource(rows=rows)
    payload = ResponsePayload.from_channel_resource(resource)  # type: ignore
    assert payload.channel_id == resource.metadata.channel_id
    assert payload.rows == rows
    assert payload.op_details["provided_rows"] == len(rows)
    assert payload.op_details["available_rows"] == resource.row_nb
    # strict False: rows should remain
    assert payload.rows is not None


def test_from_channel_resource_strict_removes_rows():
    rows = [{"a": 1}]
    resource = MockChannelResource(rows=rows)
    payload = ResponsePayload.from_channel_resource(resource, strict=True)  # type: ignore
    # rows removed, columns generated
    assert payload.rows is None
    assert payload.columns == {"a": [1]}


# # --- with_op_status ---
# def test_with_op_status_updates(channel_schema_fixture):
#     payload = ResponsePayload(channel_id=uuid4(), channel_schema=channel_schema_fixture)
#     payload.with_op_status(status="failed", details={"error": "something"})
#     assert payload.op_details.get("error") == "something"
#     assert getattr(payload, "op_status", None) == "failed"


# --- row/column conversion ---
def test_rows_to_columns_empty_rows(channel_schema_fixture):
    payload = ResponsePayload(channel_id=uuid4(), channel_schema=channel_schema_fixture)
    payload.rows_to_columns()
    assert payload.columns is None


def test_rows_to_columns_with_rows(channel_schema_fixture):
    payload = ResponsePayload(
        channel_id=uuid4(), channel_schema=channel_schema_fixture, rows=[{"x": 1, "y": 2}, {"x": 3, "y": 4}]
    )
    payload.rows_to_columns()
    assert payload.columns == {"x": [1, 3], "y": [2, 4]}
    # strict=False leaves rows intact
    assert payload.rows is not None
    payload.rows_to_columns(strict=True)
    assert payload.rows is None


# --- serializers / model_dump ---
def test_serializers_strip_nulls(channel_schema_fixture):
    payload = ResponsePayload(
        channel_id=uuid4(),
        channel_schema=channel_schema_fixture,
        name=None,
        metadata=None,
        tags=None,
        rows=None,
        columns=None,
    )
    # model_dump should remove None fields
    d = payload.model_dump()
    assert "name" not in d
    assert "metadata" not in d
    assert "tags" not in d
    assert "rows" not in d
    assert "columns" not in d
