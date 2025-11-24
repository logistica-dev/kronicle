from datetime import datetime
from random import choices
from re import compile, sub
from string import ascii_lowercase, digits
from typing import Any, Literal
from uuid import UUID, uuid4

REGEX_UUID = compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")


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
        raise ValueError(f"sensor_id must be a UUIDv4, got v{uid.version}")
    return uid


def strip_quotes(v: Any) -> Any:
    return v[1:-1] if isinstance(v, str) and len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'") else v


def normalize_to_snake_case(s: str) -> str:
    return sub(r"[^\w]", "_", str(s).lower())


def normalize_column_name(s: str) -> str:
    # Basic normalization
    s = normalize_to_snake_case(s)
    # Optionally: collapse multiple underscores
    s = sub(r"_+", "_", s)
    # Strip leading/trailing underscores
    s = s.strip("_")

    if s[0].isdigit():
        # Prefix if starting with digit
        s = "col_" + s
    elif not s:
        # Generate 8-character random name
        s = "col_" + "".join(choices(ascii_lowercase + digits, k=8))
    return s


TimeSpec = Literal["seconds", "milliseconds", "microseconds"]


def now_iso_str(timespec: TimeSpec = "seconds") -> str:
    return datetime.now().astimezone().isoformat(timespec=timespec)


if __name__ == "__main__":
    here = "str_utils"
    print(here, "strip_quotes 'toto':", strip_quotes("'toto'"))
    print(here, 'strip_quotes "toto":', strip_quotes('"toto"'))
