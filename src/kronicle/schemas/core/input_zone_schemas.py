# kronicle/schemas/core/input_zone_schemas.py
from __future__ import annotations

from kronicle.schemas.input_schema import InputSchema, NamedInputSchema

mod = "in_zone"


class InputZone(NamedInputSchema):
    pass


class InputZonePatch(InputSchema):
    pass
