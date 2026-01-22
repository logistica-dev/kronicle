# tests/types/test_iso_datetime.py
from datetime import datetime, timezone

from pytest import mark, raises

from kronicle.types.iso_datetime import IsoDateTime


# ------------------------------------------------------------------
# Constructor tests
# ------------------------------------------------------------------
def test_new_no_args_returns_now():
    dt = IsoDateTime()
    assert isinstance(dt, IsoDateTime)
    assert dt.tzinfo is not None


def test_new_empty_string_returns_now():
    dt = IsoDateTime("")
    assert isinstance(dt, IsoDateTime)
    assert dt.tzinfo is not None


def test_new_multi_arg_datetime():
    dt = IsoDateTime(2025, 9, 17, 20, 0, 0)
    assert isinstance(dt, IsoDateTime)
    assert dt.year == 2025
    assert dt.month == 9
    assert dt.day == 17
    assert dt.hour == 20
    assert dt.tzinfo is not None


def test_new_string_iso():
    dt = IsoDateTime("2025-09-17T20:00:00+02:00")
    assert isinstance(dt, IsoDateTime)
    assert dt.tzinfo is not None
    assert dt.iso_str().startswith("2025-09-17T20:00:00")


def test_new_datetime_object():
    dt0 = datetime(2025, 9, 17, 20, 0, 0)
    dt = IsoDateTime(dt0)
    assert isinstance(dt, IsoDateTime)
    assert dt.tzinfo is not None


# ------------------------------------------------------------------
# ISO string methods
# ------------------------------------------------------------------
def test_iso_string_methods():
    dt = IsoDateTime(2025, 9, 17, 20, 0, 0)
    assert dt.iso_str().startswith("2025-09-17T20:00:00")
    assert dt.iso_utc().endswith("+00:00")


# ------------------------------------------------------------------
# normalize_value
# ------------------------------------------------------------------
@mark.parametrize(
    "value",
    [
        IsoDateTime(2025, 1, 1),
        datetime(2025, 1, 1),
        "2025-01-01T12:00:00+00:00",
        "2025-01-01T12:00:00",
        "2025-01-01T12:00",
        "2025-01-01T12",
        "2025-01-01",
        "2025-01",
        "2025",
        "",
    ],
)
def test_normalize_value_valid(value):
    dt = IsoDateTime.normalize_value(value)
    assert isinstance(dt, IsoDateTime)
    assert dt.tzinfo is not None
    dd = IsoDateTime(value)
    assert isinstance(dd, IsoDateTime)
    assert dd.tzinfo is not None


def test_normalize_value_invalid():
    with raises(ValueError):
        IsoDateTime.normalize_value(123.456)


# ------------------------------------------------------------------
# _parse_partial_iso
# ------------------------------------------------------------------
@mark.parametrize("s", ["2025", "2025-09", "2025-09-17", "2025-09-17-12"])
def test_parse_partial_iso(s):
    dt = IsoDateTime._parse_partial_iso(s)
    assert isinstance(dt, IsoDateTime)
    assert dt.tzinfo is not None


# ------------------------------------------------------------------
# now methods
# ------------------------------------------------------------------
def test_now_methods():
    dt = IsoDateTime.now()
    assert isinstance(dt, IsoDateTime)
    assert dt.tzinfo is not None

    dt_utc = IsoDateTime.now_utc()
    assert dt_utc.tzinfo is not None
    assert dt_utc.tzinfo == timezone.utc

    dt_local = IsoDateTime.now_local()
    assert dt_local.tzinfo is not None

    s_log = IsoDateTime.now_log()
    assert isinstance(s_log, str)

    s_iso = IsoDateTime.now_iso_str()
    assert s_iso.endswith("+00:00")


# ------------------------------------------------------------------
# Pydantic v2 integration
# ------------------------------------------------------------------
def test_pydantic_parsing():
    from pydantic import BaseModel

    class Event(BaseModel):
        timestamp: IsoDateTime

    # naive string input
    e1 = Event(timestamp="2025-09-17T20:00:00")  # type: ignore
    assert isinstance(e1.timestamp, IsoDateTime)
    assert e1.timestamp.tzinfo is not None

    # aware string input
    e2 = Event(timestamp="2025-09-17T20:00:00+02:00")  # type: ignore
    assert isinstance(e2.timestamp, IsoDateTime)
    assert e2.timestamp.tzinfo is not None

    # datetime object input
    import datetime as dt_mod

    e3 = Event(timestamp=dt_mod.datetime(2025, 9, 17, 20, 0, 0))  # type: ignore
    assert isinstance(e3.timestamp, IsoDateTime)
    assert e3.timestamp.tzinfo is not None

    # JSON serialization
    json_out = e1.model_dump_json()
    assert isinstance(json_out, str)

    with raises(ValueError):
        IsoDateTime("2025ç0917T20:00:00+02:00")
