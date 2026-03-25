# kronicle/schemas/filters/table_query_filter.py
from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel

from kronicle.types.iso_datetime import IsoDateTime


class TableQueryFilter(BaseModel):
    id: UUID | None = None
    name: str | None = None
    date_from: IsoDateTime | None = None
    date_to: IsoDateTime | None = None
    tags: dict[str, Any] | None = None
    meta: dict[str, Any] | None = None
