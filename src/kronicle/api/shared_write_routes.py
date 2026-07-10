# kronicle/api/shared_write_routes.py
from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from kronicle.auth.auth_middleware import require_auth, require_permission
from kronicle.deps.channel_deps import channel_service
from kronicle.deps.rbac_deps import core_service, rbac_service
from kronicle.schemas.core.input_ressource_schema import InputCoreChannel
from kronicle.schemas.payload.input_payload import InputPayload
from kronicle.schemas.payload.response_payload import ResponsePayload
from kronicle.schemas.permissions.permission import PermStr
from kronicle.services.channel_service import ChannelService
from kronicle.services.core_service import CoreService
from kronicle.services.rbac_service import RbacService

"""
Routes available to users with write permissions.
These endpoints allow safe retrieval of channel metadata and stored data but mainly adding rows to
existing (or new) channel.
"""
shared_writer_router = APIRouter(dependencies=[Depends(require_auth)])


# --------------------------------------------------------------------------------------------------
# Channel CRUD (shared by setup and write routers)
# --------------------------------------------------------------------------------------------------


@shared_writer_router.post(
    "/zones/{zone_id}/channels",
    summary="Create a new channel in a zone",
    description=(
        "Create a new channel with metadata and schema inside the specified zone. "
        "Does not add rows, and fails if the channel already exists."
    ),
    response_model=ResponsePayload,
    dependencies=[Depends(require_permission(PermStr.CHANNEL_CREATE))],
)
async def create_channel_in_zone(
    zone_id: UUID,
    payload: InputPayload,
    data_service: ChannelService = Depends(channel_service),  # noqa: B008
    core: CoreService = Depends(core_service),  # noqa: B008
):
    core.ensure_channel_in_zone(InputCoreChannel.from_payload(payload), zone_id)
    return await data_service.create_channel(payload)


@shared_writer_router.patch(
    "/channels/{channel_id}",
    summary="Partially update a channel",
    description="Update only a subset of metadata, tags, or schema for the specified channel.",
    response_model=ResponsePayload,
    dependencies=[Depends(require_permission(PermStr.CHANNEL_UPDATE))],
)
async def patch_channel(
    channel_id: UUID,
    payload: InputPayload,
    data_service: ChannelService = Depends(channel_service),  # noqa: B008
    core: CoreService = Depends(core_service),  # noqa: B008
):
    payload.id = channel_id
    if not core.get_core_channel(channel_id):
        raise HTTPException(
            status_code=404,
            detail=f"Channel {channel_id} not found in RBAC; sync may be needed",
        )
    if payload.name:
        core.patch_core_channel(channel_id, name=payload.name)
    return await data_service.patch_metadata(payload)


# --------------------------------------------------------------------------------------------------
# WRITE ENDPOINTS (append-only)
# --------------------------------------------------------------------------------------------------


@shared_writer_router.post(
    "/channels/{channel_id}/rows",
    summary="Insert rows for a channel",
    description="Append-only operation: insert new rows for an existing channel. Does not modify metadata or schema.",
    response_model=ResponsePayload,
    dependencies=[Depends(require_permission(PermStr.ROW_CREATE))],
)
async def insert_rows(
    channel_id: UUID,
    payload: InputPayload,
    data_service: ChannelService = Depends(channel_service),  # noqa: B008
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
    strict: bool = Query(False, description="If true, abort on any validation error"),
):
    payload.id = channel_id  # path param overrides any payload channel_id
    response = await data_service.insert_channel_rows(payload, strict=strict)
    inserted_row_ids: list[int] = response.op_details.get("inserted_row_ids", [])  # type: ignore
    if inserted_row_ids and (payload.read_users or payload.read_groups):
        rbac.add_row_read_policies(
            channel_id=channel_id,
            timeseries_row_ids=inserted_row_ids,
            read_users=payload.read_users,
            read_groups=payload.read_groups,
        )
        response.op_details.pop("inserted_row_ids", None)
    return response
