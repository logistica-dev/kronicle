# kronicle/routes/shared_read_routes.py

from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from kronicle.controller.response_payload import ResponsePayload
from kronicle.controller.sensor_controller import SensorController
from kronicle.core.deps import get_sensor_controller
from kronicle.types.errors import BadRequestError
from kronicle.types.iso_datetime import IsoDateTime

"""
Routes available to users with read-only permissions.
These endpoints allow safe retrieval of sensor metadata and stored data.
"""
shared_read_router = APIRouter()


def parse_from_date(
    from_date: str = Query(None, description="Optional date string, return rows from this date")  # noqa: B008
) -> IsoDateTime | None:
    try:
        return None if from_date is None else IsoDateTime.normalize_value(from_date)
    except ValueError as e:
        raise BadRequestError(f"Incorrect query parameter: {e}", details={"from_date": from_date}) from e


def parse_to_date(
    to_date: str = Query(None, description="Optional date string, return rows up to this date")  # noqa: B008
) -> IsoDateTime | None:
    try:
        return None if to_date is None else IsoDateTime.normalize_value(to_date)
    except ValueError as e:
        raise BadRequestError(f"Incorrect query parameter: {e}", details={"to_date": to_date}) from e


@shared_read_router.get(
    "/channels",
    summary="list all available sensor channels",
    description=(
        "Fetches metadata for all registered sensors.\n"
        "Each entry includes schema, metadata, tags, and the number of available rows.\n"
        "No data rows are returned in this endpoint.\n"
        "Optionally, filter by a name or tag_key/tag_value pair."
    ),
    response_model=list[ResponsePayload],
)
async def fetch_all_channels_metadata(
    name: str | None = Query(None, description="Optional name to filter by"),
    tag_key: str | None = Query(None, description="Optional tag key to filter by"),
    tag_value: str | int | float | bool | None = Query(None, description="Optional tag value to filter by"),
    controller: SensorController = Depends(get_sensor_controller),  # noqa: B008
) -> list[ResponsePayload]:
    if name:
        return await controller.fetch_metadata_by_name(name=name)
    if tag_key is not None and tag_value is not None:
        return await controller.fetch_metadata_by_tag(tag_key=tag_key, tag_value=tag_value)
    return await controller.fetch_all_metadata()


@shared_read_router.get(
    "/channels/{sensor_id}",
    summary="Fetch metadata for a specific sensor channel",
    description=(
        "Retrieves metadata for the specified `sensor_id`, including schema, tags, and metadata.\n"
        "The response also includes the number of rows stored for that sensor but does not include the row data itself."
    ),
    response_model=ResponsePayload,
)
async def fetch_channel(
    sensor_id: UUID,
    controller: SensorController = Depends(get_sensor_controller),  # noqa: B008
) -> ResponsePayload:
    return await controller.fetch_metadata(sensor_id)


@shared_read_router.get(
    "/channels/{sensor_id}/rows",
    summary="Fetch stored rows for a specific sensor channel",
    description=(
        "Retrieves all stored time-series rows for the specified `sensor_id`.\n"
        "The response includes both metadata and data rows according to the sensor’s schema.\n"
    ),
    response_model=ResponsePayload,
)
async def fetch_channel_rows(
    sensor_id: UUID,
    from_date: Optional[IsoDateTime] = Depends(parse_from_date),  # noqa: B008
    to_date: Optional[IsoDateTime] = Depends(parse_to_date),  # noqa: B008
    skip_received: bool = Query(True, description="Optional flag, True to display data reception date"),
    controller: SensorController = Depends(get_sensor_controller),  # noqa: B008
) -> ResponsePayload:
    if from_date and to_date and from_date > to_date:
        raise BadRequestError("Parameter `from_date` must be earlier than `to_date` parameter")
    return await controller.fetch_rows(sensor_id, from_date=from_date, to_date=to_date, skip_received=skip_received)


@shared_read_router.get(
    "/channels/{sensor_id}/columns",
    summary="Fetch the data as columns for a specific sensor channel",
    description=(
        "Retrieves all stored time-series rows for the specified `sensor_id` and present them as columns.\n"
        "The response includes both metadata and data columns according to the sensor’s schema.\n"
    ),
    response_model=ResponsePayload,
)
async def fetch_channel_columns(
    sensor_id: UUID,
    from_date: Optional[IsoDateTime] = Depends(parse_from_date),  # noqa: B008
    to_date: Optional[IsoDateTime] = Depends(parse_to_date),  # noqa: B008
    controller: SensorController = Depends(get_sensor_controller),  # noqa: B008
) -> ResponsePayload:
    if from_date and to_date and from_date > to_date:
        raise BadRequestError("Parameter `from_date` must be earlier than `to_date` parameter")
    return await controller.fetch_columns(sensor_id, from_date=from_date, to_date=to_date)
