# tests/unit/schemas/payload/test_input_payload.py
from uuid import uuid4

import pytest
from pydantic import ValidationError

from kronicle.db.data.models.channel_schema import ChannelSchema
from kronicle.errors.error_types import BadRequestError
from kronicle.schemas.payload.input_payload import InputPayload
from kronicle.utils.str_utils import SQL_COL_MAX_LEN


# ------------------------------------------------------
# Basic instantiation
# ------------------------------------------------------
def test_input_payload_defaults():
    payload = InputPayload()
    assert payload.metadata is None
    assert payload.tags is None
    assert payload.strict is False
    assert payload.channel_id is None
    assert payload.rows is None


def test_input_payload_with_all_fields():
    schema_dict = {"time": "datetime", "temperature": "float"}
    cid = uuid4()
    payload = InputPayload(
        channel_id=cid,
        channel_schema=schema_dict,
        name="MyChannel",
        metadata={"unit": "C"},
        tags={"room": 101},
        rows=[{"time": "2025-01-01T00:00:00Z", "temperature": 20.5}],
    )
    assert payload.channel_id == cid
    assert payload.channel_schema == schema_dict
    assert payload.name
    assert payload.name == "mychannel"
    assert payload.metadata == {"unit": "C"}
    assert payload.tags == {"room": 101}
    assert payload.rows
    assert len(payload.rows) == 1


# ------------------------------------------------------
# Field validation: ensure dict
# ------------------------------------------------------
def test_metadata_tags_channel_schema_dict_or_none():
    payload = InputPayload(metadata=None, tags=None, channel_schema=None)
    assert payload.metadata == {}
    assert payload.tags == {}
    assert payload.channel_schema == {}


def test_metadata_invalid_type_raises():
    with pytest.raises(BadRequestError):
        InputPayload(metadata=["not", "a", "dict"])  # type: ignore


def test_tags_invalid_type_raises():
    with pytest.raises(BadRequestError):
        InputPayload(tags=["not", "a", "dict"])  # type: ignore


def test_channel_schema_invalid_type_raises():
    with pytest.raises(BadRequestError):
        InputPayload(channel_schema="not a dict")  # type: ignore


# ------------------------------------------------------
# Name normalization and length
# ------------------------------------------------------
def test_name_normalization():
    payload = InputPayload(name="MyChannel")
    assert isinstance(payload.name, str)
    assert payload.name == "mychannel"
    assert len(payload.name) == 9


def test_name_normalization_prefix():
    payload = InputPayload(name="33")
    assert isinstance(payload.name, str)
    assert payload.name.startswith("channel_")
    assert len(payload.name) == 10


def test_name_too_long_raises():
    long_name = "x" * 100
    payload = InputPayload(name=long_name)
    assert isinstance(payload.name, str)
    assert len(payload.name) == SQL_COL_MAX_LEN


# ------------------------------------------------------
# ensure_channel_id
# ------------------------------------------------------
def test_ensure_channel_id_success():
    cid = uuid4()
    payload = InputPayload(channel_id=cid)
    assert payload.ensure_channel_id() == cid


def test_ensure_channel_id_missing_raises():
    payload = InputPayload()
    with pytest.raises(BadRequestError):
        payload.ensure_channel_id()


# ------------------------------------------------------
# ensure_channel_rows
# ------------------------------------------------------
def test_ensure_channel_rows_success():
    rows = [{"time": "t", "temp": 20}]
    payload = InputPayload(rows=rows)
    assert payload.ensure_channel_rows() == rows


def test_ensure_channel_rows_missing_raises():
    payload = InputPayload()
    with pytest.raises(BadRequestError):
        payload.ensure_channel_rows()


def test_ensure_channel_rows_not_list_raises():
    with pytest.raises(ValidationError):
        InputPayload(rows="not a list")  # type: ignore


# ------------------------------------------------------
# ensure_channel_schema
# ------------------------------------------------------
def test_ensure_channel_schema_success(monkeypatch):
    schema_dict = {"time": "datetime", "temperature": "float"}
    payload = InputPayload(channel_schema=schema_dict)

    # Patch ChannelSchema.from_user_json to return a dummy object
    dummy_schema = ChannelSchema.from_user_json(schema_dict=schema_dict)
    assert payload.ensure_channel_schema() == dummy_schema


def test_ensure_channel_schema_missing_raises():
    payload = InputPayload()
    with pytest.raises(BadRequestError):
        payload.ensure_channel_schema()


def test_ensure_channel_schema_invalid_type_raises():
    with pytest.raises(BadRequestError):
        InputPayload(channel_schema="not a dict")  # type: ignore


# ------------------------------------------------------
# _populate_name model validator
# ------------------------------------------------------
def test_populate_name_with_alias():
    payload = InputPayload._populate_name({"channel_name": "AliasName"})  # type: ignore
    assert "name" in payload
    assert payload["name"] == "AliasName"


def test_populate_name_with_name_preserved():
    values = {"name": "Original"}
    payload = InputPayload._populate_name(values)  # type: ignore
    assert payload["name"] == "Original"


# ------------------------------------------------------
# Example payload config
# ------------------------------------------------------
def test_model_config_has_example():
    config = dict(InputPayload.model_config)
    assert isinstance(config, dict)
    json_schema_extra = config.get("json_schema_extra")
    assert json_schema_extra is not None
    assert isinstance(json_schema_extra, dict)
    assert "example" in json_schema_extra.keys()
    ex = json_schema_extra["example"]
    assert "channel_id" in ex
    assert "channel_schema" in ex
    assert "rows" in ex
