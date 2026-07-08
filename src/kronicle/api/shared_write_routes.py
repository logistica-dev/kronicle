# kronicle/api/shared_write_routes.py
from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from kronicle.auth.auth_middleware import require_auth, require_permission
from kronicle.deps.channel_deps import channel_service
from kronicle.deps.rbac_deps import core_service
from kronicle.schemas.payload.input_payload import InputPayload
from kronicle.schemas.payload.response_payload import ResponsePayload
from kronicle.schemas.permissions.permission import PermStr
from kronicle.services.channel_service import ChannelService
from kronicle.services.core_service import CoreService
from kronicle.utils.str_utils import ensure_uuid4

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
    core.ensure_channel_in_zone(ensure_uuid4(payload.id), zone_id)
    return await data_service.create_channel(payload)


@shared_writer_router.post(
    "/channels",
    summary="Update or create a channel",
    description=(
        "Upsert a channel: if it exists, updates metadata (schema must match if provided); "
        "if it does not exist, creates it with the given schema and metadata."
    ),
    response_model=ResponsePayload,
    dependencies=[Depends(require_permission(PermStr.CHANNEL_UPDATE))],
)
async def create_channel(
    payload: InputPayload,
    data_service: ChannelService = Depends(channel_service),  # noqa: B008
    core: CoreService = Depends(core_service),  # noqa: B008
):
    res = await data_service.create_channel(payload)
    core.ensure_channel_in_zone(res.channel_id, core.ensure_default_zone().id)
    return res


@shared_writer_router.put(
    "/channels",
    summary="Update or create a channel",
    description=(
        "Upsert a channel: if it exists, updates metadata (schema must match if provided); "
        "if it does not exist, creates it with the given schema and metadata."
    ),
    response_model=ResponsePayload,
    dependencies=[Depends(require_permission(PermStr.CHANNEL_UPDATE))],
)
async def upsert_channel(
    payload: InputPayload,
    data_service: ChannelService = Depends(channel_service),  # noqa: B008
    core: CoreService = Depends(core_service),  # noqa: B008
):
    if payload.id:
        core.ensure_channel_in_zone(payload.id, core.ensure_default_zone().id)
    return await data_service.upsert_metadata(payload)


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
    summary="Insert rows for a  channel",
    description="Append-only operation: insert new rows for an existing channel. Does not modify metadata or schema.",
    response_model=ResponsePayload,
    dependencies=[Depends(require_permission(PermStr.ROW_CREATE))],
)
async def insert_rows(
    channel_id: UUID,
    payload: InputPayload,
    data_service: ChannelService = Depends(channel_service),  # noqa: B008
    strict: bool = Query(False, description="If true, abort on any validation error"),
):
    payload.id = channel_id  # path param overrides any payload channel_id
    return await data_service.insert_channel_rows(payload, strict=strict)
