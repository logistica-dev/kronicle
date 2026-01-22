# kronicle/utils/str_utils.py

from base64 import urlsafe_b64decode, urlsafe_b64encode
from random import choices
from re import compile, fullmatch, sub
from string import ascii_lowercase, digits
from typing import Any
from uuid import UUID, uuid4

from kronicle.types.tag_type import TagType

REGEX_UUID = compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")


def enforce_length(here: str, length=12) -> str:
    if len(here) < length:
        return here.ljust(length, " ")
    else:
        return here[0:length]


def uuid4_str() -> str:
    return str(uuid4())


def tiny_id(n: int = 8) -> str:
    if n < 1:
        n = 8
    return uuid4_str().replace("-", "")[0:n]


def is_uuid_v4(id: str | UUID) -> bool:
    if id is None or not isinstance(id, (str, UUID)):
        return False
    try:
        uuid4 = UUID(str(id))
        return True if uuid4.version == 4 else False
    except ValueError:
        return False


def check_is_uuid4(id: str | UUID) -> str:
    if id is None:
        raise ValueError("Input parameter should not be null")
    if not isinstance(id, (str, UUID)):
        raise ValueError(f"Input parameter is not a valid UUID v4: '{id}'")
    try:
        uuid_v4 = UUID(str(id))
        if uuid_v4.version == 4:
            return str(uuid_v4)
    except ValueError:
        pass
    raise ValueError(f"Input parameter is not a valid UUID v4: '{id}'")


def ensure_uuid4(id) -> UUID:
    try:
        uid = UUID(str(id))  # normalize
    except Exception as e:
        raise ValueError(f"Invalid UUID format: {id}") from e
    if uid.version != 4:
        raise ValueError(f"channel_id must be a UUIDv4, got v{uid.version}")
    return uid


def generate_uuid4() -> UUID:
    return uuid4()


def strip_quotes(v: Any) -> Any:
    return v[1:-1] if isinstance(v, str) and len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'") else v


def normalize_to_snake_case(s: str) -> str:
    return sub(r"[^\w]", "_", str(s).lower())


def normalize_name(s: str, prefix: str | None = "") -> str:
    if not isinstance(s, str):
        raise TypeError("Name should be a string")
    if s is None or len(s) < 1:
        raise ValueError("Name cannot be empty")
    try:
        # Basic normalization
        s = normalize_to_snake_case(s)
        # Optionally: collapse multiple underscores
        s = sub(r"_+", "_", s)
        # Strip leading/trailing underscores
        s = s.strip("_")
        if len(s) < 1:
            raise ValueError("Name is invalid")
        if s[0].isdigit():
            if not prefix:
                raise ValueError("Name is invalid.")
            s = prefix + s
        elif not s:
            if not prefix:
                raise ValueError("Name is invalid..")
            # Generate 8-character random name
            s = prefix + "".join(choices(ascii_lowercase + digits, k=8))
        return s
    except Exception as e:
        raise ValueError("Name is invalid...") from e


def extract_tags(tags: list[str]):
    """
    Input: list of "key:value" strings
    Simple "key" will be parsed as "key":True
    """
    tag_dict: dict[str, TagType] = {}
    for t in tags:
        if ":" not in t:
            tag_dict[t] = True
            continue

        key, value = t.split(":", 1)
        try:
            key = normalize_name(key)
        except (ValueError, TypeError):
            continue
        # Optional: cast value to int/float/bool if possible
        if (val_i := value.lower()) in {"true", "false"}:
            cast_value: TagType = val_i == "true"
        elif value.isdigit():
            cast_value: TagType = int(value)
        else:
            try:
                cast_value = float(value)
            except ValueError:
                cast_value = value
        tag_dict[key] = cast_value
    return tag_dict


def sanitize_dict(
    d: dict[str, Any] | None = None,
    field_name: str = "",
    cast_values: bool = True,
) -> dict[str, Any]:
    """
    Validate and normalize dictionaries.

    - Ensures all keys are non-empty strings.
    - Optionally casts non-primitive values to string.
    - Restricts values to (str, int, float, list).

    Raises:
        ValueError: If an invalid key is encountered.
    """
    out: dict[str, Any] = {}

    for k, v in (d or {}).items():
        try:
            k_norm = normalize_name(k)
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid {field_name + ' ' if field_name else ''}key: {k}") from e
        if not isinstance(k_norm, str) or not k_norm:
            raise ValueError(f"Invalid {field_name + ' ' if field_name else ''}key: {k}")
        if cast_values and not isinstance(v, TagType):
            v = str(v)
        out[k] = v
    return out


# --------------------------------------------------------------------------------------------------
# Base 64 strings
# --------------------------------------------------------------------------------------------------
def pad_b64_str(jwt_base64url: str) -> str:
    """Adds equal signs at the end of the string for its length to reach a multiple of 4"""
    jwt_str_length = len(jwt_base64url)
    _, mod = divmod(jwt_str_length, 4)
    return jwt_base64url if mod == 0 else jwt_base64url.ljust(jwt_str_length + 4 - mod, "=")


def is_base64_url(sb) -> bool:
    """
    Check if an input is urlsafe-base64 encoded
    :param sb: a string or a bytes
    source: https://stackoverflow.com/a/45928164
    """
    try:
        if isinstance(sb, str):
            # If there's any unicode here, an exception will be thrown and the function will return false
            sb_bytes = bytes(sb, "ascii")
        elif isinstance(sb, bytes):
            sb_bytes = sb
        else:
            raise ValueError("Argument must be string or bytes")
        return urlsafe_b64encode(urlsafe_b64decode(sb_bytes)) == sb_bytes
    except Exception:
        return False


def decode_b64url(b64_str: str) -> str:
    padded_str = pad_b64_str(b64_str)
    if not is_base64_url(padded_str):
        raise ValueError("Input string should be encoded in base64url")
    try:
        return urlsafe_b64decode(padded_str).decode("utf-8")
    except Exception as e:
        raise ValueError("Input string is not properly encoded in base64url") from e


def q_ident(name: str) -> str:
    """
    Safely quote a PostgreSQL identifier.

    This function wraps the given identifier in double quotes and escapes any
    embedded double quotes according to PostgreSQL rules. It is intended for
    safely interpolating database object names (e.g., roles, databases,
    tables) into dynamically constructed SQL statements.

    Note:
        - This is for identifiers only, not values.
        - SQL values (e.g., passwords) must still be passed as query parameters.
        - Does not validate identifier semantics; it only ensures proper quoting.

    Args:
        name: The identifier to quote.

    Returns:
        A safely quoted PostgreSQL identifier.
    """
    return '"' + name.replace('"', '""') + '"'


def validate_pg_identifier(name: str) -> str:
    if not fullmatch(r"[a-zA-Z_]\w+", name):
        raise ValueError(f"Invalid Postgres identifier '{name}': only alphanumerics and underscores allowed")
    return name


if __name__ == "__main__":  # pragma: no cover
    here = "str_utils"
    print(here, "strip_quotes 'testsing':", strip_quotes("'testsing'"))
    print(here, 'strip_quotes "testsing":', strip_quotes('"testsing"'))

    for arg in ["testing", "YrmFjpOshYMxl0tth73NhYmtl4GFzew"]:
        print(here, "* is_base64_url", f"'{arg}'? ->", is_base64_url(arg))
        arg = pad_b64_str(arg)
        print(here, "* is_base64_url", f"'{arg}'? ->", is_base64_url(arg))
    print(here, "PHVzcj46PHB3ZD4 ->", decode_b64url("PHVzcj46PHB3ZD4"))

    print("q_ident", q_ident("testsing'testsinge''o\"o"))
    print(isinstance(["ert", 4], TagType))

    print(extract_tags(["one:two", "three", "four:False", "five:5", "6:six", "seven:True", "eight:8.8"]))
    try:
        print(normalize_name(6))  # type: ignore
    except TypeError:
        print("OK: TypeError caught")
    try:
        print(normalize_name("6", ""))
    except ValueError:
        print("OK: ValueError caught")
