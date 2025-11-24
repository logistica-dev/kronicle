# kronicle/controller/input_payloads.py
from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from kronicle.db.sensor_schema import SensorSchema
from kronicle.types.errors import BadRequestError
from kronicle.types.iso_datetime import IsoDateTime
from kronicle.utils.dict_utils import ensure_dict_or_none
from kronicle.utils.logger import log_d
from kronicle.utils.str_utils import ensure_uuid4, tiny_id, uuid4_str


def example_payload():
    return ConfigDict(
        json_schema_extra={
            "example": {
                "sensor_id": uuid4_str(),
                "sensor_schema": {"time": "datetime", "temperature": "float", "humidity": "float"},
                "sensor_name": f"thermo-{tiny_id(5)}",
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
    User-provided payload for a sensor.
    The 'sensor_schema' field is a dict of column_name -> type string.
    """

    sensor_id: UUID | None = None
    sensor_schema: dict[str, str] | None = None
    name: str | None = None
    metadata: dict[str, Any] | None = None
    tags: dict[str, str | int | float | list] | None = None
    rows: list[dict[str, Any]] | None = None
    strict: bool = Field(default=False, description="If true, any validation error aborts the entire request.")

    model_config = example_payload()

    @field_validator("sensor_schema", "metadata", "tags", mode="before")
    @classmethod
    def ensure_dict_or_none(cls, d, info: ValidationInfo) -> dict:
        return ensure_dict_or_none(d, info.field_name)

    @model_validator(mode="before")
    @classmethod
    def _populate_sensor_name(cls, values: dict[str, Any]):
        if not values.get("name") and values.get("sensor_name"):
            values["name"] = values["sensor_name"]
        return values

    def ensure_sensor_id(self) -> UUID:
        if not self.sensor_id:
            raise BadRequestError(
                "Missing required parameter",
                details={"sensor_id": "Provide a valid sensor_id UUID in the request payload"},
            )
        return ensure_uuid4(self.sensor_id)

    def ensure_sensor_rows(self) -> list[dict[str, Any]]:
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

    def ensure_sensor_schema(self) -> SensorSchema:
        if not self.sensor_schema:
            raise BadRequestError(
                "Missing required parameter", details={"sensor_schema": "Provide a schema in the request payload"}
            )
        if not isinstance(self.sensor_schema, dict):
            raise BadRequestError(
                "Incorrect type for field 'sensor_schema", details={"sensor_schema": "Should be a json/dict"}
            )
        return SensorSchema.from_user_json(self.sensor_schema)


# --------------------------------------------------------------------------------------------------
# Base Payload
# --------------------------------------------------------------------------------------------------
# class Payload(BaseModel):
#     """
#     Base payload object for sensor operations.
#     - sensor_id: UUID of the sensor
#     - sensor_schema: optional schema (used for new sensors)
#     - metadata: key -> value describing the sensor (str|int|float)
#     - tags: key -> value for indexing/searching (str|int|float)
#     - rows: optional list of sensor data rows
#     """


# --------------------------------------------------------------------------------------------------
# Simple test / sanity check
# --------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    here = "in_payload.test"
    from uuid import uuid4

    from pydantic import ValidationError

    from kronicle.utils.logger import log_d

    log_d(here, "=== sensor_payloads.py main test ===")

    # --- create sample schema from InputSchema ---
    input_schema = {"temperature": "number", "humidity": "float", "time": "datetime"}

    # --- create input payload ---
    payload = InputPayload(
        sensor_id=uuid4(),
        sensor_schema=input_schema,
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
        bad_payload = InputPayload(sensor_id=uuid4(), metadata={"": "empty key"})
    except ValueError as e:
        log_d(here, "Caught expected sanitization error :", e)

    # --- test validator: tags must be dict ---
    try:
        InputPayload(sensor_id=uuid4(), tags=["not", "a", "dict"])  # type: ignore
    except (ValidationError, TypeError) as e:
        log_d(here, "Caught expected validation error :")
        log_d(here, e)

    # --- test validator: empty dicts default ---
    empty_payload = InputPayload(sensor_id=uuid4())
    log_d(here, "Empty InputPayload (metadata/tags default to dict) :", empty_payload.model_dump())

    log_d(here, "=== End of sensor_payloads.py test ===")

    # --- test validator: no uuid ---
    empty_payload = InputPayload()  # type: ignore
    log_d(here, "No ID InputPayload (metadata/tags default to dict) :", empty_payload.model_dump())
