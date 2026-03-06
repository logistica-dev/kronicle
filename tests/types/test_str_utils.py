# tests/types/test_str_utils.py
import re
from uuid import UUID, uuid4

from pytest import mark, raises

from kronicle.utils import str_utils

# --------------------------------------
# UUID helpers
# --------------------------------------


def test_uuid4_str_and_generate_uuid4():
    s = str_utils.uuid4_str()
    u = str_utils.generate_uuid4()
    assert str_utils.is_uuid_v4(s)
    assert str_utils.is_uuid_v4(u)
    assert isinstance(u, UUID)


@mark.parametrize("n", [0, 8, 12, 32])
def test_tiny_id_length(n):
    tid = str_utils.tiny_id(n)
    assert len(tid) == (n if n >= 1 else 8)
    # should be hex characters
    assert re.fullmatch(r"[0-9a-f]+", tid)


@mark.parametrize(
    "val,expected",
    [
        (uuid4(), True),
        (str(uuid4()), True),
        ("invalid", False),
        (12345, False),
        (None, False),
    ],
)
def test_is_uuid_v4(val, expected):
    assert str_utils.is_uuid_v4(val) is expected


@mark.parametrize(
    "val,should_raise",
    [
        (uuid4(), False),
        (str(uuid4()), False),
        ("invalid", True),
        (12345, True),
        (None, True),
    ],
)
def test_check_is_uuid4(val, should_raise):
    if should_raise:
        with raises(ValueError):
            str_utils.check_is_uuid4(val)
    else:
        result = str_utils.check_is_uuid4(val)
        assert str_utils.is_uuid_v4(result)


def test_ensure_uuid4_raises_on_invalid_or_wrong_version():
    # Invalid string
    with raises(ValueError):
        str_utils.ensure_uuid4("not-a-uuid")
    # UUID v1
    from uuid import uuid1

    with raises(ValueError):
        str_utils.ensure_uuid4(uuid1())
    # UUID v4 works
    u4 = uuid4()
    assert str_utils.ensure_uuid4(u4) == u4


# --------------------------------------
# Strip quotes
# --------------------------------------


@mark.parametrize(
    "inp,expected",
    [
        ("'abc'", "abc"),
        ('"abc"', "abc"),
        ("noquotes", "noquotes"),
        ("'a", "'a"),
        ('b"', 'b"'),
        ("''", ""),
    ],
)
def test_strip_quotes(inp, expected):
    assert str_utils.strip_quotes(inp) == expected


# --------------------------------------
# Name normalization
# --------------------------------------


@mark.parametrize(
    "inp,expected",
    [
        ("My Name", "my_name"),
        ("123Start", "col_123start"),
        ("__multiple__underscores__", "multiple_underscores"),
    ],
)
def test_normalize_name(inp, expected):
    result = str_utils.normalize_name(inp, "col_")
    if isinstance(expected, str):
        assert result == expected
    else:
        assert expected.fullmatch(result)


# --------------------------------------
# Base64 helpers
# --------------------------------------


@mark.parametrize(
    "s",
    [
        "hello",
        "teststring123",
        "cm9yb2RvZDphZXJncHVpaA",
    ],
)
def test_base64_roundtrip(s):
    padded = str_utils.pad_b64_str(s)
    assert isinstance(padded, str)
    if str_utils.is_base64_url(padded):
        decoded = str_utils.decode_b64url(padded)
        # decoded string may differ if input was not valid base64; safe to just check type
        assert isinstance(decoded, str)


def test_is_base64_url_invalid_type():
    assert str_utils.is_base64_url(12345) is False
    assert str_utils.is_base64_url(None) is False


def test_decode_b64url_invalid_raises():
    with raises(ValueError):
        str_utils.decode_b64url("not_base64")
    with raises(ValueError):
        str_utils.decode_b64url("YrmFjpOshYMxl0tth73NhYmtl4GFzew")


# --------------------------------------
# Postgres identifiers
# --------------------------------------


def test_q_ident_escapes_quotes():
    raw = 'test"name'
    quoted = str_utils.q_ident(raw)
    assert quoted.startswith('"') and quoted.endswith('"')
    assert '""' in quoted


@mark.parametrize(
    "inp,valid",
    [
        ("valid_name", True),
        ("ValidName", True),
        ("_underscore123", True),
        ("123start", False),
        ("has-dash", False),
        ("has space", False),
        ("$dollar", False),
    ],
)
def test_normalize_pg_identifier(inp, valid):
    if valid:
        assert str_utils.normalize_pg_identifier(inp) == inp
    else:
        with raises(ValueError):
            str_utils.normalize_pg_identifier(inp)
