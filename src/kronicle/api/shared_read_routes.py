# kronicle/api/shared_read_routes.py
from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from kronicle.auth.auth_middleware import require_auth
from kronicle.deps.channel_deps import channel_service
from kronicle.schemas.filters.request_filter import RequestFilter
from kronicle.schemas.payload.response_payload import ResponsePayload
from kronicle.services.channel_service import ChannelService

"""
Routes available to users with read-only permissions.
These endpoints allow safe retrieval of channel metadata and stored data.
"""
shared_read_router = APIRouter(dependencies=[Depends(require_auth)])


# def parse_from_date(
#     from_date: str = Query(None, description="Optional date string, return rows from this date")  # noqa: B008
# ) -> IsoDateTime | None:
#     try:
#         return None if from_date is None else IsoDateTime.normalize_value(from_date)
#     except ValueError as e:
#         raise BadRequestError(f"Incorrect query parameter: {e}", details={"from_date": from_date}) from e


# def parse_to_date(
#     to_date: str = Query(None, description="Optional date string, return rows up to this date")  # noqa: B008
# ) -> IsoDateTime | None:
#     try:
#         return None if to_date is None else IsoDateTime.normalize_value(to_date)
#     except ValueError as e:
#         raise BadRequestError(f"Incorrect query parameter: {e}", details={"to_date": to_date}) from e


@shared_read_router.get(
    "/channels",
    summary="list all available channels",
    description=(
        "Fetches metadata for all registered channels.\n"
        "Each entry includes schema, metadata, tags, and the number of available rows.\n"
        "No data rows are returned in this endpoint.\n"
        "Optionally, filter by a name or tag_key/tag_value pair."
    ),
    response_model=list[ResponsePayload],
)
async def fetch_all_channels_metadata(
    name: str | None = Query(None, description="Optional name to filter by"),
    tags: list[str] = Query(None, description="Optional tags as key:value pairs, e.g., color:red"),  # noqa: B008
    controller: ChannelService = Depends(channel_service),  # noqa: B008
) -> list[ResponsePayload] | ResponsePayload:
    # Name filter takes priority
    if name:
        return await controller.fetch_metadata_by_name(name=name)
    # Tags filter
    if tags:
        return await controller.fetch_metadata_by_tags(tags=tags)
    return await controller.fetch_all_metadata()


@shared_read_router.get(
    "/channels/{channel_id}",
    summary="Fetch metadata for a specific channel",
    description=(
        "Retrieves metadata for the specified `channel_id`, including schema, tags, and metadata.\n"
        "The response also includes the number of rows stored for that channel\n"
        "but does not include the row data itself."
    ),
    response_model=ResponsePayload,
)
async def fetch_channel(
    channel_id: UUID,
    controller: ChannelService = Depends(channel_service),  # noqa: B008
) -> ResponsePayload:
    return await controller.fetch_metadata(channel_id)


@shared_read_router.get(
    "/channels/{channel_id}/rows",
    summary="Fetch stored rows for a specific channel",
    description=(
        "Retrieves all stored time-series rows for the specified `channel_id`.\n"
        "Filtering, ordering, pagination, and column selection are supported via query parameters.\n"
        "The response includes both metadata and data rows according to the channel’s schema.\n"
    ),
    response_model=ResponsePayload,
)
async def fetch_channel_rows(
    channel_id: UUID,
    filter: Annotated[RequestFilter, Depends()],
    controller: ChannelService = Depends(channel_service),  # noqa: B008
) -> ResponsePayload:
    return await controller.fetch_rows(channel_id, filter=filter)


@shared_read_router.get(
    "/channels/{channel_id}/columns",
    summary="Fetch the data as columns for a specific channel",
    description=(
        "Retrieves all stored time-series rows for the specified `channel_id` and present them as columns.\n"
        "The response includes both metadata and data columns according to the channel’s schema.\n"
    ),
    response_model=ResponsePayload,
)
async def fetch_channel_columns(
    channel_id: UUID,
    filter: Annotated[RequestFilter, Depends()],
    controller: ChannelService = Depends(channel_service),  # noqa: B008
) -> ResponsePayload:
    return await controller.fetch_columns(channel_id, filter=filter)
