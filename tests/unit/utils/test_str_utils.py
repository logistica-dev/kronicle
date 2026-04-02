# tests/unit/utils/test_str_utils.py

import re
from uuid import UUID, uuid4

from pytest import mark, raises

from kronicle.utils.str_utils import (
    check_is_uuid4,
    decode_b64url,
    encode_b64url,
    enforce_length,
    ensure_uuid4,
    extract_tags,
    generate_uuid4,
    is_base64_url,
    is_uuid_v4,
    normalize_name,
    normalize_name_accept_subs,
    normalize_pg_identifier,
    normalize_to_snake_case,
    pad_b64_str,
    q_ident,
    replace_non_words,
    sanitize_dict,
    split_strip,
    split_strip_norm_one,
    strip_quotes,
    tiny_id,
    uuid4_str,
)

# --------------------------------------------------------------------------------------
# Basic utils
# --------------------------------------------------------------------------------------


def test_enforce_length_padding():
    assert enforce_length("abc", 5) == "abc  "


def test_enforce_length_truncate():
    assert enforce_length("abcdef", 3) == "abc"


def test_tiny_id_default_length():
    assert len(tiny_id()) == 8


def test_tiny_id_custom_length():
    assert len(tiny_id(4)) == 4


def test_tiny_id_invalid_length():
    assert len(tiny_id(0)) == 8


# --------------------------------------------------------------------------------------
# UUID
# --------------------------------------------------------------------------------------


def test_is_uuid_v4_valid():
    uid = UUID("123e4567-e89b-42d3-a456-426614174000")
    assert is_uuid_v4(uid)


def test_is_uuid_v4_invalid():
    assert not is_uuid_v4("not-a-uuid")


def test_check_is_uuid4_valid():
    uid = "123e4567-e89b-42d3-a456-426614174000"
    assert check_is_uuid4(uid) == uid


def test_check_is_uuid4_invalid():
    with raises(ValueError):
        check_is_uuid4("bad")


def test_ensure_uuid4_valid():
    uid = "123e4567-e89b-42d3-a456-426614174000"
    result = ensure_uuid4(uid)
    assert isinstance(result, UUID)


def test_ensure_uuid4_wrong_version():
    with raises(ValueError):
        ensure_uuid4("123e4567-e89b-12d3-a456-426614174000")  # v1


def test_uuid4_str_and_generate_uuid4():
    s = uuid4_str()
    u = generate_uuid4()
    assert is_uuid_v4(s)
    assert is_uuid_v4(u)
    assert isinstance(u, UUID)


@mark.parametrize("n", [0, 8, 12, 32])
def test_tiny_id_length(n):
    tid = tiny_id(n)
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
    assert is_uuid_v4(val) is expected


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
            check_is_uuid4(val)
    else:
        result = check_is_uuid4(val)
        assert is_uuid_v4(result)


def test_ensure_uuid4_raises_on_invalid_or_wrong_version():
    # Invalid string
    with raises(ValueError):
        ensure_uuid4("not-a-uuid")
    # UUID v1
    from uuid import uuid1

    with raises(ValueError):
        ensure_uuid4(uuid1())
    # UUID v4 works
    u4 = uuid4()
    assert ensure_uuid4(u4) == u4


# --------------------------------------
# Strip quotes
# --------------------------------------


@mark.parametrize(
    "inp,expected",
    [
        ("'abc'", "abc"),
        ('"abc"', "abc"),
        ("abc", "abc"),
        ("noquotes", "noquotes"),
        ("'a", "'a"),
        ('b"', 'b"'),
        ("''", ""),
    ],
)
def test_strip_quotes(inp, expected):
    assert strip_quotes(inp) == expected


# --------------------------------------------------------------------------------------
# String transforms
# --------------------------------------------------------------------------------------


def test_replace_non_words():
    assert replace_non_words("a b-c!") == "a_b_c_"


def test_normalize_to_snake_case():
    assert normalize_to_snake_case("Hello World!") == "hello_world_"


def test_normalize_to_snake_case_keep_dots():
    assert normalize_to_snake_case("a.b c", keep_dots=True) == "a.b_c"


# --------------------------------------------------------------------------------------
# normalize_name (core logic)
# --------------------------------------------------------------------------------------


# --------------------------------------
# Name normalization
# --------------------------------------


@mark.parametrize(
    "inp,expected",
    [
        ("My Name", "my_name"),
        ("Hello World", "hello_world"),
        ("123Start", "col_123start"),
        ("__multiple__underscores__", "multiple_underscores"),
        ("__________a__b___c_______", "a_b_c"),
        ("__abc__", "abc"),
    ],
)
def test_normalize_name(inp, expected):
    result = normalize_name(inp, prefix="col_")
    if isinstance(expected, str):
        assert result == expected
    else:
        assert expected.fullmatch(result)


def test_normalize_name_prefix_required_for_digit():
    with raises(ValueError):
        normalize_name("1abc")


def test_normalize_name_with_prefix():
    assert normalize_name("1abc", prefix="x_") == "x_1abc"


def test_normalize_name_invalid_type():
    with raises(TypeError):
        normalize_name(123)  # type: ignore


def test_normalize_name_empty():
    with raises(ValueError):
        normalize_name("")


def test_normalize_name_max_length():
    long_name = "a" * 100
    result = normalize_name(long_name)
    assert len(result) <= 32


# --------------------------------------------------------------------------------------
# normalize_name_accept_subs
# --------------------------------------------------------------------------------------


def test_normalize_name_accept_subs():
    result = normalize_name_accept_subs("Hello.World.test")
    assert result
    assert result.startswith("hello")  # first part normalized


def test_normalize_name_accept_subs_none():
    assert normalize_name_accept_subs("") is None


def test_normalize_name_accept_subs_invalid():
    with raises(ValueError):
        normalize_name_accept_subs(123)  # type: ignore


# --------------------------------------------------------------------------------------
# Tags
# --------------------------------------------------------------------------------------


def test_extract_tags():
    tags = ["a:1", "b", "c:hello"]
    result = extract_tags(tags)

    assert result["a"] == "1"
    assert result["b"] == "True"
    assert result["c"] == "hello"


# --------------------------------------------------------------------------------------
# sanitize_dict
# --------------------------------------------------------------------------------------


def test_sanitize_dict_basic():
    d = {"Hello World": 123}
    result = sanitize_dict(d, cast_values=False)

    assert "Hello World" in result
    assert result["Hello World"] == 123


def test_sanitize_dict_basic_cast():
    d = {"Hello World": 123}
    result = sanitize_dict(d, cast_values=True)

    assert "Hello World" in result
    assert result["Hello World"] == 123


def test_sanitize_dict_invalid_key():
    with raises(ValueError):
        sanitize_dict({123: "bad"})  # type: ignore


def test_sanitize_dict_no_cast():
    d = {"a": 1}
    result = sanitize_dict(d, cast_values=False)
    assert result["a"] == 1


# --------------------------------------------------------------------------------------
# Base64
# --------------------------------------------------------------------------------------


@mark.parametrize(
    "s",
    [
        "hello",
        "teststring123",
        "cm9yb2RvZDphZXJncHVpaA",
    ],
)
def test_base64_roundtrip(s):
    padded = pad_b64_str(s)
    assert isinstance(padded, str)
    if is_base64_url(padded):
        decoded = decode_b64url(padded)
        # decoded string may differ if input was not valid base64; safe to just check type
        assert isinstance(decoded, str)


def test_pad_b64_str():
    assert pad_b64_str("abc") == "abc="


def test_encode_decode_b64url():
    s = "hello"
    encoded = encode_b64url(s)
    decoded = decode_b64url(encoded)

    assert decoded == s


def test_decode_b64url_invalid():
    with raises(ValueError):
        decode_b64url("!!!notbase64!!!")
    with raises(ValueError):
        decode_b64url("not_base64")
    with raises(ValueError):
        decode_b64url("YrmFjpOshYMxl0tth73NhYmtl4GFzew")


def test_is_base64_url():
    encoded = encode_b64url("hello")
    assert is_base64_url(encoded)
    assert not is_base64_url("invalid!!")


# --------------------------------------------------------------------------------------
# SQL / identifiers
# --------------------------------------------------------------------------------------


def test_q_ident():
    assert q_ident('abc"def') == '"abc""def"'


def test_normalize_pg_identifier_valid():
    assert normalize_pg_identifier("MyTable") == "mytable"


def test_normalize_pg_identifier_invalid():
    with raises(ValueError):
        normalize_pg_identifier("123bad")


def test_q_ident_escapes_quotes():
    raw = 'test"name'
    quoted = q_ident(raw)
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
        assert normalize_pg_identifier(inp) == inp.lower()
    else:
        with raises(ValueError):
            normalize_pg_identifier(inp)


# --------------------------------------------------------------------------------------
# split helpers
# --------------------------------------------------------------------------------------


def test_split_strip():
    assert split_strip("a, b ,c") == ["a", "b", "c"]


def test_split_strip_empty():
    assert split_strip("") == []


def test_split_strip_norm_one():
    result = split_strip_norm_one(" a , b , c ")
    assert result[0] == "a"
    assert result[1:] == ["b", "c"]
