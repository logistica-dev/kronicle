# kronicle/routes/write_routes.py

from fastapi import APIRouter, Depends, Query

from kronicle.api.routes.shared_read_routes import shared_read_router
from kronicle.api.routes.shared_write_routes import shared_writer_router
from kronicle.controller.input_payloads import InputPayload
from kronicle.controller.response_payload import ResponsePayload
from kronicle.controller.sensor_controller import SensorController
from kronicle.core.deps import get_sensor_controller

writer_router = APIRouter(tags=["Input data"])

# --------------------------------------------------------------------------------------------------
# READ-ONLY ENDPOINTS
# --------------------------------------------------------------------------------------------------
writer_router.include_router(shared_read_router)


# --------------------------------------------------------------------------------------------------
# WRITE ENDPOINTS (append-only)
# --------------------------------------------------------------------------------------------------
writer_router.include_router(shared_writer_router)


@writer_router.post(
    "/channels",
    summary="Upsert metadata and insert rows",
    description="Append-only operation: creates new metadata if missing and inserts sensor data rows",
    response_model=ResponsePayload,
)
async def upsert_metadata_and_rows(
    payload: InputPayload,
    controller: SensorController = Depends(get_sensor_controller),  # noqa: B008
    strict: bool = Query(False, description="If true, abort on any validation error"),
):
    return await controller.upsert_metadata_and_insert_rows(payload, strict)
