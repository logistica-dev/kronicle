# kronicle/routes/write_routes.py
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from kronicle.controller.input_payloads import InputPayload
from kronicle.controller.response_payload import ResponsePayload
from kronicle.controller.sensor_controller import SensorController
from kronicle.core.deps import get_sensor_controller

"""
Routes available to users with write permissions.
These endpoints allow safe retrieval of sensor metadata and stored data but mainly adding rows to
existing (or new) sensor channel.
"""
shared_writer_router = APIRouter()


# --------------------------------------------------------------------------------------------------
# WRITE ENDPOINTS (append-only)
# --------------------------------------------------------------------------------------------------


@shared_writer_router.post(
    "/channels/{sensor_id}/rows",
    summary="Insert rows for a sensor",
    description="Append-only operation: insert new rows for an existing sensor. Does not modify metadata or schema.",
    response_model=ResponsePayload,
)
async def insert_rows(
    sensor_id: UUID,
    payload: InputPayload,
    controller: SensorController = Depends(get_sensor_controller),  # noqa: B008
    strict: bool = Query(False, description="If true, abort on any validation error"),
):
    payload.sensor_id = sensor_id  # path param overrides any payload sensor_id
    return await controller.insert_sensor_rows(payload, strict=strict)
