# tests/unit/schemas/test_output_schema.py
from uuid import uuid4

from kronicle.schemas.output_schema import OutputSchema


def test_model_dump_serializes_and_excludes_none():
    s = OutputSchema(id=uuid4(), name="tintin")
    d = s.model_dump()
    assert d["name"] == "tintin"
    assert "details" not in d  # exclude_none default
    assert isinstance(d["id"], str)  # mode="json"


def test_model_dump_json_returns_json_string():
    s = OutputSchema(id=uuid4(), name="tintin")
    js = s.model_dump_json()
    assert isinstance(js, str)
    assert "tintin" in js


def test_str_uses_base_model_dump_json():
    s = OutputSchema(id=uuid4(), name="tintin")
    assert str(s) == super(OutputSchema, s).model_dump_json(exclude_none=True)
