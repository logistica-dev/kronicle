# tests/unit/schemas/payload/test_processed_payload.py

from uuid import uuid4

import pytest

from kronicle.db.data.models.channel_schema import ChannelSchema
from kronicle.errors.error_types import BadRequestError
from kronicle.schemas.payload.input_payload import InputPayload
from kronicle.schemas.payload.processed_payload import ProcessedPayload


def _schema() -> ChannelSchema:
    return ChannelSchema.from_user_json({"time": "time"})


def test_from_input_generates_channel_id_when_missing():
    processed = ProcessedPayload.from_input(InputPayload(name="my channel"), _schema())
    assert processed.channel_id is not None
    assert processed.channel_id.version == 4


def test_from_input_honors_provided_channel_id():
    channel_id = uuid4()
    processed = ProcessedPayload.from_input(InputPayload(id=channel_id, name="my channel"), _schema())
    assert processed.channel_id == channel_id


def test_from_input_carries_column_metadata():
    processed = ProcessedPayload.from_input(
        InputPayload(
            channel_schema={"temperature": "float"},
            column_metadata={"temperature": {"unit": "Cel", "description": "Air temperature"}},
        ),
        _schema(),
    )
    assert processed.column_metadata == {"temperature": {"unit": "Cel", "description": "Air temperature"}}


# ------------------------------------------------------
# sanitize_metadata
# ------------------------------------------------------
def test_sanitize_metadata_valid():
    meta = {"unit": "C", "location": "lab", "nested": {"floor": 3}}
    assert ProcessedPayload.sanitize_metadata(meta) == meta


def test_sanitize_metadata_none():
    assert ProcessedPayload.sanitize_metadata(None) == {}


def test_sanitize_metadata_too_deep_raises():
    deep = {"a": {"b": {"c": {"d": {"e": {"f": 1}}}}}}
    with pytest.raises(BadRequestError, match="Invalid metadata"):
        ProcessedPayload.sanitize_metadata(deep)


def test_sanitize_metadata_nested_non_string_key_raises():
    bad = {"a": {1: "value"}}
    with pytest.raises(BadRequestError, match="Invalid metadata"):
        ProcessedPayload.sanitize_metadata(bad)


def test_sanitize_metadata_unsupported_type_raises():
    bad = {"a": b"bytes"}
    with pytest.raises(BadRequestError, match="Invalid metadata"):
        ProcessedPayload.sanitize_metadata(bad)


# ------------------------------------------------------
# sanitize_tags
# ------------------------------------------------------
def test_sanitize_tags_valid():
    tags = {"room": 101, "unit": "C"}
    assert ProcessedPayload.sanitize_tags(tags) == tags


def test_sanitize_tags_cast_values():
    tags = {"active": True, "count": 3, "label": "x"}
    assert ProcessedPayload.sanitize_tags(tags) == tags


def test_sanitize_tags_unsupported_type_raises():
    bad = {"a": b"bytes"}
    with pytest.raises(BadRequestError, match="Invalid tags"):
        ProcessedPayload.sanitize_tags(bad)


# ------------------------------------------------------
# sanitize_column_metadata
# ------------------------------------------------------
def test_sanitize_column_metadata_valid():
    col_meta = {"temperature": {"unit": "Cel", "description": "Air temperature"}}
    assert ProcessedPayload.sanitize_column_metadata(col_meta) == col_meta


def test_sanitize_column_metadata_none():
    assert ProcessedPayload.sanitize_column_metadata(None) == {}


def test_sanitize_column_metadata_passes_keys_through_unchanged():
    # sanitize_dict validates + normalizes keys for checks but keeps the original key;
    # containment against the channel schema is enforced later by ChannelMetadata.
    raw = {"Temperature": {"unit": "Cel"}, "humidity": {"unit": "%"}}
    assert ProcessedPayload.sanitize_column_metadata(raw) == raw


def test_sanitize_column_metadata_too_deep_raises():
    deep = {"temperature": {"a": {"b": {"c": {"d": {"e": {"f": 1}}}}}}}
    with pytest.raises(BadRequestError, match="Invalid column_metadata"):
        ProcessedPayload.sanitize_column_metadata(deep)


def test_sanitize_column_metadata_nested_non_string_key_raises():
    bad = {"temperature": {1: "value"}}
    with pytest.raises(BadRequestError, match="Invalid column_metadata"):
        ProcessedPayload.sanitize_column_metadata(bad)
