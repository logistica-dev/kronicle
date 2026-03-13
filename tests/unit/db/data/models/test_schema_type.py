from uuid import UUID

from pytest import mark, raises

from kronicle.db.data.models.schema_registry import SchemaRegistry
from kronicle.db.data.models.schema_types import SchemaType
from kronicle.types.iso_datetime import IsoDateTime

registry = SchemaRegistry()


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def valid_sample_for(canonical: str):
    return {
        "str": "hello",
        "int": 42,
        "float": 3.14,
        "bool": True,
        "uuid": UUID("3768b5c9-c951-4184-a4b0-78debb5f61dc"),
        "datetime": "2025-09-17T22:30:00+02:00",
        "dict": {"a": 1},
        "list": [1, 2, 3],
    }[canonical]


def invalid_sample_for(canonical: str):
    return {
        "str": 123,
        "int": "abc",
        "float": "abc",
        "bool": "true",
        "uuid": "3768b5c9",
        "datetime": "not-a-date",
        "dict": "not_dict",
        "list": "not_list",
    }[canonical]


# ------------------------------------------------------------------
# Property-style canonical coverage
# ------------------------------------------------------------------


@mark.parametrize("canonical", registry.allowed_types)
def test_canonical_roundtrip_and_mappings(canonical):
    # SchemaType construction
    t = SchemaType(canonical)

    # Canonical should remain stable
    assert t.name == canonical

    # Registry mappings must be consistent
    assert registry.canonical_to_db(canonical) == t.db_type
    assert registry.canonical_to_py(canonical) == t.py_type


@mark.parametrize("canonical", registry.allowed_types)
def test_validation_success_for_valid_samples(canonical):
    t = SchemaType(canonical)
    sample = valid_sample_for(canonical)

    result = t.validate(sample)

    if canonical == "float":
        # int → float coercion allowed
        assert isinstance(result, float)
    elif canonical == "datetime":
        assert isinstance(result, IsoDateTime)
    else:
        assert isinstance(result, registry.canonical_to_py(canonical))


@mark.parametrize("canonical", registry.allowed_types)
def test_validation_fails_for_invalid_samples(canonical):
    t = SchemaType(canonical)
    bad_value = invalid_sample_for(canonical)

    with raises(ValueError):
        t.validate(bad_value)


@mark.parametrize("canonical", registry.allowed_types)
def test_optional_variant_accepts_none(canonical):
    t = SchemaType(f"optional[{canonical}]")
    assert t.validate(None) is None
    assert t.normalize_value(None) is None


@mark.parametrize("canonical", registry.allowed_types)
def test_non_optional_rejects_none(canonical):
    t = SchemaType(canonical)

    with raises(ValueError):
        t.validate(None)

    with raises(ValueError):
        t.normalize_value(None)


def test_from_str():
    t = SchemaType.from_str("string")
    assert t.name == "str"
    assert not t.optional


def test_optional_syntax_parsing():
    t = SchemaType("optional[int]")
    assert t.name == "int"
    assert t.optional is True


def test_invalid_type_raises_value_error():
    with raises(ValueError):
        SchemaType("does_not_exist")


def test_db_and_py_type_properties():
    t = SchemaType("float")
    assert t.db_type == "DOUBLE PRECISION"
    assert t.py_type is float


def test_validate_success():
    t = SchemaType("int")
    assert t.validate(10) == 10


def test_validate_optional_none():
    t = SchemaType("optional[str]")
    assert t.validate(None) is None


def test_validate_optional_datetime():
    t = SchemaType("optional[datetime]")
    assert t.validate(IsoDateTime())


def test_validate_none_not_optional():
    t = SchemaType("str")
    with raises(ValueError):
        t.validate(None)


def test_validate_type_error_converted_to_bad_request():
    t = SchemaType("float")
    with raises(ValueError):
        t.validate("not_float")


def test_normalize_datetime():
    t = SchemaType("datetime")
    value = t.normalize_value("2025-09-17T22:30:00+02:00")
    assert isinstance(value, IsoDateTime)


def test_normalize_none_optional():
    t = SchemaType("optional[int]")
    assert t.normalize_value(None) is None


def test_normalize_none_not_optional():
    t = SchemaType("int")
    with raises(ValueError):
        t.normalize_value(None)


def test_is_json():
    assert SchemaType("dict").is_json() is True
    assert SchemaType("list").is_json() is True
    assert SchemaType("str").is_json() is False


def test_equality_with_schema_type():
    assert SchemaType("str") == SchemaType("string")
    assert SchemaType("str") != SchemaType("int")
    assert SchemaType("str") != 10


def test_equality_with_string():
    assert SchemaType("str") == "str"
    assert SchemaType("optional[str]") == "optional[str]"
    assert SchemaType("str") != "string"


def test_repr():
    s = SchemaType("double precision")
    # Note the quotes around the name because of !r
    assert repr(s) == "SchemaType('float', optional=False)"


def test_repr_optional_true():
    s = SchemaType("double precision", optional=True)
    assert repr(s) == "SchemaType('float', optional=True)"
