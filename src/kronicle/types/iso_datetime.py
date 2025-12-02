# kronicle/types/iso_datetime.py
from __future__ import annotations

from datetime import datetime, timezone, tzinfo
from typing import Any

from kronicle.types.errors import BadRequestError

# --------------------------------------------------------------------------------------------------
# System local timezone
# --------------------------------------------------------------------------------------------------
_LOCAL_TZ = datetime.now().astimezone().tzinfo


# --------------------------------------------------------------------------------------------------
# IsoDateTime class
# --------------------------------------------------------------------------------------------------
class IsoDateTime(datetime):
    """
    Datetime subclass that always stringifies as ISO 8601 with local offset.
    Naive datetimes default to system local timezone.
    """

    def __new__(cls, *args, **kwargs):
        dt = super().__new__(cls, *args, **kwargs)
        # If naive, default to local tz
        return cls.to_iso_datetime(dt)

    # ------------------------------------------------------------------
    # String / repr output
    # ------------------------------------------------------------------
    def __str__(self) -> str:
        return self.iso_str()

    def __repr__(self) -> str:
        # return f"IsoDateTime({self.iso_str()!r})"
        return f"{self.iso_str()!r}"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def iso_str(self) -> str:
        """Return ISO string normalized to UTC (+00:00)."""
        return self.astimezone(tz=self.tzinfo).isoformat()

    def iso_utc(self) -> str:
        """Return ISO string normalized to UTC (+00:00)."""
        return self.astimezone(tz=timezone.utc).isoformat()

    def iso_local(self) -> str:
        """Return ISO string with local offset preserved."""
        return self.astimezone().isoformat()

    @classmethod
    def now(cls, tz: tzinfo | None = None) -> IsoDateTime:
        return cls.to_iso_datetime(super().now(tz))

    @classmethod
    def now_utc(cls) -> IsoDateTime:
        return cls.now(timezone.utc)

    @classmethod
    def now_local(cls) -> IsoDateTime:
        return cls.now(_LOCAL_TZ)

    @classmethod
    def now_log(cls) -> str:
        return cls.now_local().strftime("%Y-%m-%d %H:%M:%S")

    @classmethod
    def to_iso_datetime(cls, dt: datetime) -> IsoDateTime:
        if isinstance(dt, IsoDateTime):
            return dt
        return cls(
            dt.year,
            dt.month,
            dt.day,
            dt.hour,
            dt.minute,
            dt.second,
            dt.microsecond,
            tzinfo=_LOCAL_TZ if dt.tzinfo is None else dt.tzinfo,
        )

    @classmethod
    def _parse_partial_iso(cls, value: str) -> IsoDateTime:
        parts = value.split("-")
        year = int(parts[0])
        month = int(parts[1]) if len(parts) > 1 else 1
        day = int(parts[2]) if len(parts) > 2 else 1
        return IsoDateTime(year, month, day)

    @classmethod
    def normalize_value(cls, value: Any, to_local_tz: bool = False) -> IsoDateTime:
        """
        Normalize a value to IsoDateTime:
        - IsoDateTime: keep as is
        - datetime: convert to IsoDateTime, preserve tz if present, else default local
        - str: parse ISO string, naive -> local tz
        """
        if isinstance(value, IsoDateTime):
            dt = value
            return dt.astimezone() if to_local_tz else dt

        if isinstance(value, datetime):
            dt = cls.to_iso_datetime(value)
            return dt.astimezone() if to_local_tz else dt

        if isinstance(value, str):
            try:
                dt = cls.fromisoformat(value.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = cls.to_iso_datetime(dt)  # apply local tz
                return dt.astimezone() if to_local_tz else dt
            except ValueError:
                try:
                    return cls._parse_partial_iso(value)
                except ValueError as e:
                    raise ValueError(f"Cannot parse datetime from '{value}'") from e
        raise BadRequestError(f"Cannot normalize type '{type(value).__name__}' to datetime")

    # ------------------------------------------------------------------
    # Pydantic v2 integration
    # ------------------------------------------------------------------
    @classmethod
    def __get_pydantic_core_schema__(cls, source: Any, handler: Any):

        from pydantic_core import core_schema

        validator_schema = core_schema.no_info_after_validator_function(
            schema=core_schema.datetime_schema(),
            function=cls.to_iso_datetime,
        )

        return core_schema.chain_schema(
            steps=[validator_schema],
            serialization=core_schema.plain_serializer_function_ser_schema(lambda v, _: v.iso_local(), info_arg=True),
        )


# --------------------------------------------------------------------------------------------------
# Main test
# --------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    here = "iso_datetime.test"
    from time import sleep

    from pydantic import BaseModel

    from kronicle.utils.dev_logs import log_d

    log_d(here, "=== iso_datetime.py main test ===")

    # -------------------------
    # Basic naive datetime
    # -------------------------
    naive = IsoDateTime(2025, 9, 17, 20, 0, 0)
    log_d(here, "Naive input defaults to local tz", naive, repr(naive))
    log_d(here, "iso_utc:", naive.iso_utc())
    log_d(here, "iso_local:", naive.iso_local())

    # -------------------------
    # System-aware datetime
    # -------------------------
    aware = IsoDateTime.now()
    log_d(here, "Aware input with system tz", aware, repr(aware))
    log_d(here, "iso_utc:", aware.iso_utc())
    log_d(here, "iso_local:", aware.iso_local())

    log_d(here, "Now UTC:", IsoDateTime.now_utc())
    log_d(here, "now_local:", IsoDateTime.now_local())

    # -------------------------
    # Auto-increment / sleep test
    # -------------------------
    sleep(1)
    later = IsoDateTime.now()
    log_d(here, "Later datetime shows offset too", later)

    # -------------------------
    # Pydantic v2 model tests
    # -------------------------

    class Event(BaseModel):
        timestamp: IsoDateTime

    # naive string input
    e1 = Event(timestamp="2025-09-17T20:00:00")  # type: ignore
    log_d(here, "Pydantic naive input parsed", e1.timestamp, repr(e1.timestamp))
    assert isinstance(e1.timestamp, IsoDateTime)
    assert e1.timestamp.tzinfo is not None

    # aware string input
    e2 = Event(timestamp="2025-09-17T20:00:00+02:00")  # type: ignore
    log_d(here, "Pydantic aware input parsed", e2.timestamp, repr(e2.timestamp))
    assert isinstance(e2.timestamp, IsoDateTime)
    assert e2.timestamp.tzinfo is not None

    # datetime object input
    import datetime as dt_mod

    e3 = Event(timestamp=dt_mod.datetime(2025, 9, 17, 20, 0, 0))  # type: ignore
    log_d(here, "Pydantic datetime object input", e3.timestamp, repr(e3.timestamp))
    assert isinstance(e3.timestamp, IsoDateTime)
    assert e3.timestamp.tzinfo is not None

    # JSON serialization
    json_out = e1.model_dump_json()
    log_d(here, "Pydantic JSON serialization", json_out)

    # Normalization
    log_d(here, "Normalization from datetime", IsoDateTime.normalize_value(e1.timestamp))
    log_d(here, "Normalization from datetime", IsoDateTime.normalize_value(e2.timestamp))
    log_d(here, "Normalization from datetime", IsoDateTime.normalize_value(e3.timestamp))
    log_d(here, "Normalization from datetime", IsoDateTime.normalize_value("2025-09-17T20:00:00+02:00"))
    log_d(here, "Normalization from datetime", IsoDateTime.normalize_value("2025-09-17 20:00+00:00"))
    log_d(here, "Normalization from datetime", IsoDateTime.normalize_value("2025-09-17 20:00+00"))
    log_d(here, "Normalization from datetime", IsoDateTime.normalize_value("2025-09-17 20:00"))
    log_d(here, "Normalization from datetime", IsoDateTime.normalize_value("2025-09-17 20"))

    d = IsoDateTime.normalize_value("2025-09-17 20")
    log_d(here, "Simple", d)
    now = IsoDateTime.now_local()
    log_d(here, "Now", now)

    log_d(here, "=== End of iso_datetime.py test ===")
