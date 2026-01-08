# kronicle/controller/input_payloads.py
from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, ValidationInfo, field_validator, model_validator

from kronicle.db.channel_schema import ChannelSchema
from kronicle.errors.error_types import BadRequestError
from kronicle.types.iso_datetime import IsoDateTime
from kronicle.utils.dict_utils import ensure_dict_or_none
from kronicle.utils.str_utils import ensure_uuid4, tiny_id, uuid4_str


def example_payload():
    return ConfigDict(
        json_schema_extra={
            "example": {
                "channel_id": uuid4_str(),
                "channel_schema": {"time": "datetime", "temperature": "float", "humidity": "float"},
                "channel_name": f"thermo-{tiny_id(5)}",
                "metadata": {"unit": "C"},
                "tags": {"location": "lab", "floor": 3},
                "rows": [
                    {"time": "2025-01-01T00:00:00Z", "temperature": 20.5, "humidity": 55.1},
                    {"time": "2025-01-02T00:00:00Z", "temperature": 20.0, "humidity": 54.1},
                    {"time": "2025-01-03T00:00:00Z", "temperature": 20.5, "humidity": 53.1},
                    {"time": "2025-01-04T00:00:00Z", "temperature": 29.5, "humidity": 52.1},
                ],
            }
        }
    )


# --------------------------------------------------------------------------------------------------
# InputPayload
# --------------------------------------------------------------------------------------------------
class InputPayload(BaseModel):
    """
    User-provided payload for a channel.
    The 'channel_schema' field is a dict of column_name -> type string.
    """

    channel_id: UUID | None = None
    channel_schema: dict[str, str] | None = None
    name: str | None = None
    metadata: dict[str, Any] | None = None
    tags: dict[str, str | int | float | bool | list] | None = None
    rows: list[dict[str, Any]] | None = None
    strict: bool = Field(default=False, description="If true, any validation error aborts the entire request.")

    model_config = example_payload()

    # --------------------------------------------------------------------------
    # Ensure dict fields
    # --------------------------------------------------------------------------
    @field_validator("channel_schema", "metadata", "tags", mode="before")
    @classmethod
    def ensure_dict_or_none(cls, d, info: ValidationInfo) -> dict:
        return ensure_dict_or_none(d, info.field_name)

    # --------------------------------------------------------------------------
    # Populate name from channel_name (safe)
    # --------------------------------------------------------------------------
    @model_validator(mode="before")
    @classmethod
    def _populate_channel_name(cls, values):
        # Only operate on dictionaries
        if not isinstance(values, dict):
            return values

        # Accept user-provided alias
        if "name" not in values and "channel_name" in values:
            values["name"] = values["channel_name"]
        return values

    # --------------------------------------------------------------------------
    # Runtime validation helpers
    # --------------------------------------------------------------------------
    def ensure_channel_id(self) -> UUID:
        if not self.channel_id:
            raise BadRequestError(
                "Missing required parameter",
                details={"channel_id": "Provide a valid channel_id UUID in the request payload"},
            )
        return ensure_uuid4(self.channel_id)

    def ensure_channel_rows(self) -> list[dict[str, Any]]:
        if not self.rows:
            raise BadRequestError(
                "Missing required type for field 'rows'",
                details={"rows": "Provide rows of data in the request payload"},
            )
        if not isinstance(self.rows, list):
            raise BadRequestError(
                "Incorrect field",
                details={"rows": 'Should be a list of {"field1": val1, "field2": val2} json/dict elements'},
            )
        return self.rows

    def ensure_channel_schema(self) -> ChannelSchema:
        if not self.channel_schema:
            raise BadRequestError(
                "Missing required parameter", details={"channel_schema": "Provide a schema in the request payload"}
            )
        if not isinstance(self.channel_schema, dict):
            raise BadRequestError(
                "Incorrect type for field 'channel_schema", details={"channel_schema": "Should be a json/dict"}
            )
        return ChannelSchema.from_user_json(self.channel_schema)


# --------------------------------------------------------------------------------------------------
# Base Payload
# --------------------------------------------------------------------------------------------------
# class Payload(BaseModel):
#     """
#     Base payload object for channel operations.
#     - channel_id: UUID of the channel
#     - channel_schema: optional schema (used for new channels)
#     - metadata: key -> value describing the channel (str|int|float)
#     - tags: key -> value for indexing/searching (str|int|float)
#     - rows: optional list of channel data rows
#     """


# --------------------------------------------------------------------------------------------------
# Simple test / sanity check
# --------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    here = "in_payload.test"
    from uuid import uuid4

    from kronicle.utils.dev_logs import log_d

    log_d(here, "=== channel_payloads.py main test ===")

    # --- create sample schema from InputSchema ---
    input_schema = {"temperature": "number", "humidity": "float", "time": "datetime"}

    # --- create input payload ---
    payload = InputPayload(
        channel_id=uuid4(),
        channel_schema=input_schema,
        metadata={"location": "lab", "unit": "C"},
        tags={"room": 101},
        rows=[
            {"time": IsoDateTime.now_local().isoformat(), "temperature": 22.5, "humidity": 55.0},
            {"time": IsoDateTime.now_local(), "temperature": 23.0, "humidity": 53.0},
        ],
    )
    log_d(here, "InputPayload OK :", payload)

    # --- test sanitization: bad metadata key ---
    try:
        bad_payload = InputPayload(channel_id=uuid4(), metadata={"": "empty key"})
    except ValueError as e:
        log_d(here, "Caught expected sanitization error :", e)

    # --- test validator: tags must be dict ---
    try:
        InputPayload(channel_id=uuid4(), tags=["not", "a", "dict"])  # type: ignore
    except (ValidationError, TypeError) as e:
        log_d(here, "Caught expected validation error :")
        log_d(here, e)

    # --- test validator: empty dicts default ---
    empty_payload = InputPayload(channel_id=uuid4())
    log_d(here, "Empty InputPayload (metadata/tags default to dict) :", empty_payload.model_dump())

    log_d(here, "=== End of channel_payloads.py test ===")

    # --- test validator: no uuid ---
    empty_payload = InputPayload()  # type: ignore
    log_d(here, "No ID InputPayload (metadata/tags default to dict) :", empty_payload.model_dump())
