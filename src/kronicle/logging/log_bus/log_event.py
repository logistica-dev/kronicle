# kronicle/logging/log_event.py
from typing import Any

from pydantic import BaseModel, Field

from kronicle.types.iso_datetime import IsoDateTime


class LogEvent(BaseModel):
    source: str  # e.g. "api", "controller", "db"
    action: str  # e.g. "insert_rows", "fetch_metadata"
    level: str  # "DEBUG", "INFO", "WARN", "ERROR"
    message: str  # human-readable text
    details: dict[str, Any] = {}  # structured payload
    timestamp: IsoDateTime = Field(default_factory=IsoDateTime.now_local)
