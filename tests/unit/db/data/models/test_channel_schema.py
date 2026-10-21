# tests/unit/db/data/models/test_channel_schema.py
from pytest import raises

from kronicle.db.data.models.channel_schema import ChannelSchema
from kronicle.db.data.models.schema_types import SchemaType
from kronicle.types.iso_datetime import IsoDateTime

# ------------------------------------------------------------------
# Schema creation & sanitization
# ------------------------------------------------------------------


def test_empty_schema_raises():
    with raises(ValueError):
        ChannelSchema.sanitize_user_schema({})


def test_basic_schema_sanitization():
    schema = ChannelSchema.sanitize_user_schema({"Temperature": "float", "Humidity": "int"})

    assert "temperature" in schema.column_types
    assert "humidity" in schema.column_types

    assert isinstance(schema.column_types["temperature"], SchemaType)
    assert schema.column_types["temperature"].name == "float"


def test_time_column_forced_to_datetime():
    schema = ChannelSchema.sanitize_user_schema({"time": "int", "value": "float"})  # should be overridden

    assert schema.column_types["time"].name == "datetime"


def test_received_at_is_skipped():
    schema = ChannelSchema.sanitize_user_schema({"received_at": "datetime", "value": "float"})

    assert "received_at" not in schema.column_types
    assert "value" in schema.column_types


def test_duplicate_normalized_column_raises():
    with raises(ValueError):
        ChannelSchema.sanitize_user_schema({"Temp": "float", " temp ": "int"})  # normalizes to same


def test_reserved_keyword_raises():
    with raises(ValueError):
        ChannelSchema.sanitize_user_schema({"select": "int"})


def test_non_string_column_name_raises():
    with raises(ValueError):
        ChannelSchema.sanitize_user_schema({123: "int"})  # type: ignore


def test_blank_column_name_raises():
    with raises(ValueError):
        ChannelSchema.sanitize_user_schema({"   ": "int"})


def test_only_received_at_schema_raises():
    # every column skipped -> the sanitized schema stays empty
    with raises(ValueError, match="Schema cannot be empty"):
        ChannelSchema.sanitize_user_schema({"received_at": "datetime"})


def test_unnormalizable_column_name_raises(monkeypatch):
    # `normalize_name` is patched to return a falsy name: defensive branch of `_sanitize_column`
    monkeypatch.setattr(
        "kronicle.db.data.models.channel_schema.normalize_name",
        lambda *args, **kwargs: "",
    )
    with raises(ValueError, match="Incorrect column name"):
        ChannelSchema.sanitize_user_schema({"temp": "float"})


# ------------------------------------------------------------------
# Mapping & serialization
# ------------------------------------------------------------------


def test_ordered_columns():
    schema = ChannelSchema.sanitize_user_schema({"temp": "float", "count": "int"})
    assert schema.ordered_columns == ["time", "temp", "count", "received_at"]


def test_user_to_db_mapping():
    schema = ChannelSchema.sanitize_user_schema(
        {
            "Temperature C": "float",
        }
    )

    db_col = next(iter(schema.column_types))
    assert schema.get_usr_col_name(db_col) == "Temperature C"


def test_to_user_json():
    schema = ChannelSchema.sanitize_user_schema({"temp": "float"})

    user_json = schema.to_user_json()
    assert user_json == {"temp": "float"}


def test_to_db_json():
    schema = ChannelSchema.sanitize_user_schema({"temp": "float"})

    db_json = schema.to_db_json()
    assert db_json == {"temp": "DOUBLE PRECISION"}


def test_model_dump_flatten():
    schema = ChannelSchema.sanitize_user_schema({"temp": "float"})

    dumped = schema.model_dump()
    assert dumped == {"temp": "float"}


def test_model_dump_unflattened():
    schema = ChannelSchema.sanitize_user_schema({"temp": "float"})

    dumped = schema.model_dump(flatten=False)
    assert dumped["column_types"] == {"temp": {"name": "float", "optional": False}}


def test_model_dump_json():
    from json import loads

    schema = ChannelSchema.sanitize_user_schema({"temp": "float"})

    assert loads(schema.model_dump_json()) == {"temp": "float"}


def test_from_db_json_reconstructs_schema():
    schema = ChannelSchema.from_db_json({"temp": "DOUBLE PRECISION", "count": "INTEGER"})

    assert schema.column_types["temp"].name == "float"
    assert schema.column_types["count"].name == "int"
    # no user names stored in DB: db name is its own user name
    assert schema.db_to_usr == {"temp": "temp", "count": "count"}
    assert schema.to_user_json() == {"temp": "float", "count": "int"}


# ------------------------------------------------------------------
# Structural comparison
# ------------------------------------------------------------------


def test_equivalent_to_ignores_user_names():
    s1 = ChannelSchema.sanitize_user_schema({"Temp": "float"})
    s2 = ChannelSchema.sanitize_user_schema({"temp": "float"})

    assert s1.equivalent_to(s2)


def test_equivalent_to_false_if_type_changes():
    s1 = ChannelSchema.sanitize_user_schema({"temp": "float"})
    s2 = ChannelSchema.sanitize_user_schema({"temp": "int"})

    assert not s1.equivalent_to(s2)


def test_equivalent_to_non_schema_is_false():
    s1 = ChannelSchema.sanitize_user_schema({"temp": "float"})
    assert not s1.equivalent_to({"temp": "float"})  # type: ignore[arg-type]


def test_diff_added_removed_changed():
    s1 = ChannelSchema.sanitize_user_schema({"aa": "int", "bb": "float"})
    s2 = ChannelSchema.sanitize_user_schema({"bb": "float", "cc": "int"})

    diff = s1.diff(s2)

    assert "cc" in diff["added"]
    assert "aa" in diff["removed"]
    assert diff["changed"] == {}


# ------------------------------------------------------------------
# Row validation
# ------------------------------------------------------------------


def test_validate_row_success():
    schema = ChannelSchema.sanitize_user_schema({"temp": "float", "count": "int"})

    row = {"temp": 1.5, "count": 3}

    validated = schema.validate_row(row)

    assert "time" in validated
    assert "received_at" in validated
    assert validated["temp"] == 1.5
    assert validated["count"] == 3


def test_validate_row_missing_required_column():
    schema = ChannelSchema.sanitize_user_schema({"temp": "float"})

    with raises(ValueError):
        schema.validate_row({})  # missing temp


def test_validate_row_optional_column_missing():
    schema = ChannelSchema.sanitize_user_schema({"temp": "optional[float]"})

    validated = schema.validate_row({})
    assert "temp" in validated
    assert validated["temp"] is None  # optional is set to None


def test_validate_row_accepts_user_column_name():
    schema = ChannelSchema.sanitize_user_schema({"Temperature C": "float"})

    row = {"Temperature C": 2.5}
    validated = schema.validate_row(row)

    assert "temperature_c" in validated
    assert validated["temperature_c"] == 2.5


def test_validate_row_invalid_type():
    schema = ChannelSchema.sanitize_user_schema({"temp": "float"})

    with raises(ValueError):
        schema.validate_row({"temp": "not_float"})


def test_validate_row_time_default_now():
    schema = ChannelSchema.sanitize_user_schema({"temp": "float"})

    validated = schema.validate_row({"temp": 1.0})
    assert isinstance(validated["time"], IsoDateTime)


def test_validate_row_time_from_input():
    schema = ChannelSchema.sanitize_user_schema({"temp": "float"})

    validated = schema.validate_row({"time": "2025-09-17T22:30:00+02:00", "temp": 1.0})

    assert isinstance(validated["time"], IsoDateTime)


def test_validate_row_received_at_from_system():
    schema = ChannelSchema.sanitize_user_schema({"temp": "float"})

    ts = IsoDateTime.now()
    validated = schema.validate_row({"temp": 1.0}, now=ts)

    assert validated["received_at"] == ts


def test_validate_row_received_at_from_internal():
    schema = ChannelSchema.sanitize_user_schema({"temp": "float"})

    ts = IsoDateTime.now_utc()

    validated = schema.validate_row({"temp": 1.0, "received_at": ts}, from_user=False)

    assert validated["received_at"] == ts


def test_validate_row_keeps_row_id_from_internal():
    schema = ChannelSchema.sanitize_user_schema({"temp": "float"})

    validated = schema.validate_row({"temp": 1.0, "row_id": 12345}, from_user=False)

    assert validated["row_id"] == 12345
    assert "row_id" not in schema.validate_row({"temp": 1.0}, from_user=False)
