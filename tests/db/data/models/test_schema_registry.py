from pytest import mark, raises

from kronicle.db.data.models.schema_registry import SchemaRegistry, SchemaTypeInfo
from kronicle.types.iso_datetime import IsoDateTime


def test_registry_is_singleton():
    r1 = SchemaRegistry()
    r2 = SchemaRegistry()
    assert r1 is r2


def test_allowed_types_contains_expected():
    registry = SchemaRegistry()
    assert set(registry.allowed_types) == {
        "str",
        "int",
        "float",
        "bool",
        "uuid",
        "datetime",
        "dict",
        "list",
    }


def test_str():
    assert (
        str(SchemaTypeInfo("str", "TEXT", str, ["string", "str", "text"]))
        == "SchemaTypeInfo(can=str,db=TEXT,py=str,aliases=['string', 'str', 'text'])"
    )


@mark.parametrize(
    "alias,canonical",
    [
        ("string", "str"),
        ("text", "str"),
        ("integer", "int"),
        ("number", "float"),
        ("double precision", "float"),
        ("boolean", "bool"),
        ("time", "datetime"),
        ("json", "dict"),
        ("list", "list"),
    ],
)
def test_user_to_canonical(alias, canonical):
    registry = SchemaRegistry()
    assert registry.user_to_canonical(alias) == canonical


def test_unknown_alias_raises():
    registry = SchemaRegistry()
    with raises(ValueError):
        registry.user_to_canonical("does_not_exist")


def test_canonical_to_db_and_py():
    registry = SchemaRegistry()

    assert registry.canonical_to_db("str") == "TEXT"
    assert registry.canonical_to_py("str") is str

    assert registry.canonical_to_db("float") == "DOUBLE PRECISION"
    assert registry.canonical_to_py("float") is float

    assert registry.canonical_to_db("datetime") == "TIMESTAMPTZ"
    assert registry.canonical_to_py("datetime") is IsoDateTime


def test_validate_none_is_none():
    type_date = SchemaTypeInfo(
        "datetime", "TIMESTAMPTZ", IsoDateTime, ["datetime", "date", "timestamp", "timestampz", "time"]
    )
    assert type_date.validate(None) is None


def test_validate_date():
    type_date = SchemaTypeInfo(
        "datetime", "TIMESTAMPTZ", IsoDateTime, ["datetime", "date", "timestamp", "timestampz", "time"]
    )
    assert str(type_date.validate("2025-09-17 20:00:00Z")) == "2025-09-17T20:00:00+00:00"


def test_get_by_canonical_raises():
    registry = SchemaRegistry()
    with raises(ValueError):
        registry.get_by_canonical("no_type")


def test_validate_float_accepts_int():
    registry = SchemaRegistry()
    value = registry.validate_value(5, "float")
    assert isinstance(value, float)
    assert value == 5.0


def test_validate_wrong_type_raises():
    registry = SchemaRegistry()
    with raises(TypeError):
        registry.validate_value("abc", "int")


def test_validate_unknown_type_raises():
    registry = SchemaRegistry()
    with raises(ValueError):
        registry.validate_value(123, "unknown_type")


def test_validate_list():
    registry = SchemaRegistry()
    assert registry.validate_value([5], "list")


def test_normalize_dict_and_list():
    registry = SchemaRegistry()

    test_dict = {"a": 1}
    test_list = [1, 2, 3]

    assert registry.normalize_value(test_dict, "dict") == test_dict
    assert registry.normalize_value(test_list, "list") == test_list

    with raises(ValueError):
        registry.normalize_value("not_dict", "dict")


def test_normalize_int():
    registry = SchemaRegistry()
    assert registry.normalize_value(5, "int") == 5
    with raises(ValueError):
        assert registry.normalize_value("r", "float")


def test_datetime_normalization_returns_iso_datetime():
    registry = SchemaRegistry()
    value = registry.normalize_value("2025-09-17T22:30:00+02:00", "datetime")
    assert isinstance(value, IsoDateTime)
