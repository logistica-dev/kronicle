# kronicle/routes/setup_routes.py

from uuid import UUID

from fastapi import APIRouter, Body, Depends

from kronicle.api.routes.shared_read_routes import shared_read_router
from kronicle.api.routes.shared_write_routes import shared_writer_router
from kronicle.controller.input_payloads import InputPayload
from kronicle.controller.response_payload import ResponsePayload
from kronicle.controller.sensor_controller import SensorController
from kronicle.core.deps import get_sensor_controller
from kronicle.types.schema_types import COL_TO_DB_TYPE

"""
Admin/setup routes:
- Create, update, delete channels
- Clone channels
- Delete all rows for a channel
- list all channels with metadata and row counts
"""
setup_router = APIRouter(tags=["Setup data channels"])


# --------------------------------------------------------------------------------------------------
# READ-ONLY ENDPOINTS
# --------------------------------------------------------------------------------------------------
setup_router.include_router(shared_read_router)


# --------------------------------------------------------------------------------------------------
# Channel metadata CRUD
# --------------------------------------------------------------------------------------------------
setup_router.include_router(shared_writer_router)


@setup_router.post(
    "/channels",
    summary="Create a new channel",
    description=(
        "Create a new sensor channel with metadata and schema."
        " Does not add rows, and fails if the channel already exists."
    ),
    response_model=ResponsePayload,
)
async def create_channel(
    payload: InputPayload,
    controller: SensorController = Depends(get_sensor_controller),  # noqa: B008
):
    return await controller.create_metadata(payload)


@setup_router.put(
    "/channels",
    summary="Update or create a channel",
    description=(
        "Upsert a channel: if it exists, updates metadata (schema must match if provided); "
        "if it does not exist, creates it with the given schema and metadata."
    ),
    response_model=ResponsePayload,
)
async def update_channel(
    payload: InputPayload,
    controller: SensorController = Depends(get_sensor_controller),  # noqa: B008
):
    return await controller.upsert_metadata(payload)


@setup_router.patch(
    "/channels/{sensor_id}",
    summary="Partially update a channel",
    description="Update only a subset of metadata, tags, or schema for the specified channel.",
    response_model=ResponsePayload,
)
async def patch_channel(
    payload: InputPayload,
    controller: SensorController = Depends(get_sensor_controller),  # noqa: B008
):
    return await controller.patch_metadata(payload)


@setup_router.post(
    "/channels/{sensor_id}/clone",
    summary="Clone a channel",
    description="Creates a new channel by cloning an existing channel's schema and optionally metadata. "
    "Does not copy data rows.",
    response_model=ResponsePayload,
)
async def clone_channel(
    sensor_id: UUID,
    payload: InputPayload,
    controller: SensorController = Depends(get_sensor_controller),  # noqa: B008
):
    return await controller.clone_channel(sensor_id, payload)


# --------------------------------------------------------------------------------------------------
# DELETE routes
# --------------------------------------------------------------------------------------------------
@setup_router.delete(
    "/channels/{sensor_id}",
    summary="Delete a channel",
    description=(
        "Deletes a channel and its metadata. "
        "All data associated with the channel is also removed. "
        "Returns the metadata of the deleted channel."
    ),
    response_model=ResponsePayload,
)
async def delete_channel(
    sensor_id: UUID,
    controller: SensorController = Depends(get_sensor_controller),  # noqa: B008
):
    return await controller.delete_sensor(sensor_id)


@setup_router.delete(
    "/channels/{sensor_id}/rows",
    summary="Delete all rows for a channel",
    description="Removes all data rows for the specified channel, while keeping its metadata intact.",
    response_model=ResponsePayload,
)
async def delete_channel_rows(
    sensor_id: UUID,
    controller: SensorController = Depends(get_sensor_controller),  # noqa: B008
):
    return await controller.delete_all_rows_for_sensor(sensor_id)


@setup_router.post(
    "/channels/batch-delete",
    summary="Delete multiple channels",
    response_model=list[ResponsePayload],
)
async def batch_delete_channels(
    payload: dict = Body(..., example={"sensor_ids": ["uuid1", "uuid2"]}),  # noqa
    controller: SensorController = Depends(get_sensor_controller),  # noqa: B008
):
    return await controller.delete_sensors(payload["sensor_ids"])


@setup_router.get(
    "/schemas/columns_types",
    summary="list the types available to describe the columns",
    description=("Retrieves every Python-like type that can be used to describe the type of a data column"),
    response_model=list[str],
)
async def get_column_types():
    return COL_TO_DB_TYPE.keys()
