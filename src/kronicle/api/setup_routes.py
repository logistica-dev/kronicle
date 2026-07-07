# kronicle/api/setup_routes.py
from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Body, Depends

from kronicle.api.shared_read_routes import shared_read_router
from kronicle.api.shared_write_routes import shared_writer_router
from kronicle.auth.auth_middleware import require_auth, require_permission, require_permission_set
from kronicle.db.data.models.schema_registry import SchemaRegistry
from kronicle.deps.channel_deps import channel_service
from kronicle.deps.rbac_deps import core_service
from kronicle.schemas.filters.row_query_filter import RowQueryFilter
from kronicle.schemas.filters.row_request_filter import RowRequestFilter
from kronicle.schemas.payload.input_payload import InputPayload
from kronicle.schemas.payload.response_payload import ResponsePayload
from kronicle.schemas.permissions.permission import PermStr
from kronicle.services.channel_service import ChannelService
from kronicle.services.core_service import CoreService

"""
Admin/setup routes:
- Create, update, delete channels
- Clone channels
- Delete all rows for a channel
- list all channels with metadata and row counts
"""
setup_router = APIRouter(
    tags=["Setup data channels"],
    dependencies=[
        Depends(require_auth),
        Depends(require_permission(PermStr.SETUP_ACCESS_PROFILE)),
    ],
)


# --------------------------------------------------------------------------------------------------
# READ-ONLY ENDPOINTS
# --------------------------------------------------------------------------------------------------
setup_router.include_router(shared_read_router)


# --------------------------------------------------------------------------------------------------
# Channel metadata CRUD
# --------------------------------------------------------------------------------------------------
setup_router.include_router(shared_writer_router)


@setup_router.post(
    "/channels/{channel_id}/clone",
    summary="Clone a channel",
    description="Creates a new channel by cloning an existing channel's schema and optionally metadata. "
    "Does not copy data rows nor name.",
    response_model=ResponsePayload,
    dependencies=[
        Depends(
            require_permission_set(
                PermStr.CHANNEL_READ,
                PermStr.CHANNEL_CREATE,
            )
        )
    ],
)
async def clone_channel(
    payload: InputPayload,
    data_service: ChannelService = Depends(channel_service),  # noqa: B008
):
    return await data_service.clone_channel(payload)


# --------------------------------------------------------------------------------------------------
# DELETE routes
# --------------------------------------------------------------------------------------------------
@setup_router.delete(
    "/channels/{channel_id}",
    summary="Delete a channel",
    description=(
        "Deletes a channel and its metadata. "
        "All data associated with the channel is also removed. "
        "Returns the metadata of the deleted channel."
    ),
    response_model=ResponsePayload,
    dependencies=[Depends(require_permission(PermStr.CHANNEL_DELETE))],
)
async def delete_channel(
    channel_id: UUID,
    data_service: ChannelService = Depends(channel_service),  # noqa: B008
    core: CoreService = Depends(core_service),  # noqa: B008
):
    core.delete_core_channel(channel_id)
    return await data_service.delete_channel(channel_id)


@setup_router.delete(
    "/channels/{channel_id}/rows",
    summary="Delete all rows for a channel",
    description="Removes all data rows for the specified channel, while keeping its metadata intact.",
    response_model=ResponsePayload,
    dependencies=[Depends(require_permission(PermStr.ROW_DELETE))],
)
async def delete_channel_rows(
    channel_id: UUID,
    filter: Annotated[RowQueryFilter, Depends()],
    data_service: ChannelService = Depends(channel_service),  # noqa: B008
):
    request_filter = RowRequestFilter.from_query(filter)
    return await data_service.delete_rows_for_channel(channel_id, filter=request_filter)


@setup_router.post(
    "/channels/batch-delete",
    summary="Delete multiple channels",
    response_model=list[ResponsePayload],
    dependencies=[Depends(require_permission(PermStr.CHANNEL_DELETE))],
)
async def batch_delete_channels(
    payload: dict = Body(..., examples=[{"channel_ids": ["uuid1", "uuid2"]}]),  # noqa
    data_service: ChannelService = Depends(channel_service),  # noqa: B008
    core: CoreService = Depends(core_service),  # noqa: B008
):
    for cid in payload["channel_ids"]:
        core.delete_core_channel(cid)
    return await data_service.delete_channels(payload["channel_ids"])


@setup_router.get(
    "/schemas/column_types",
    summary="list the types available to describe the columns",
    description=("Retrieves every Python-like type that can be used to describe the type of a data column"),
    response_model=list[str],
    dependencies=[Depends(require_permission(PermStr.CHANNEL_READ))],
)
async def get_column_types():
    return SchemaRegistry().allowed_types
