# tests/unit/schemas/payload/test_processed_payload.py

import pytest

from kronicle.errors.error_types import BadRequestError
from kronicle.schemas.payload.processed_payload import ProcessedPayload


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
