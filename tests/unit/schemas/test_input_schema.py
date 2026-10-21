# tests/unit/schemas/test_input_schema.py
from uuid import uuid4

import pytest
from pydantic import ValidationError

from kronicle.schemas.input_schema import InputSchema


def test_details_too_large_raises():
    big = {"blob": "x" * (64 * 1024)}
    with pytest.raises(ValidationError, match="too large"):
        InputSchema(details=big)


def test_falsy_details_become_none():
    s = InputSchema(details={})
    assert s.details is None


def test_valid_instance_round_trip():
    s = InputSchema(id=uuid4(), name="test", details={"a": 1})
    assert s.name == "test"
    assert s.details == {"a": 1}
