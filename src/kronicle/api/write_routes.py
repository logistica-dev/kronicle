# kronicle/api/write_routes.py
from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from kronicle.api.shared_read_routes import shared_read_router
from kronicle.api.shared_write_routes import shared_writer_router
from kronicle.auth.auth_middleware import require_auth, require_permission, require_permission_set
from kronicle.deps.channel_deps import channel_service
from kronicle.deps.rbac_deps import core_service
from kronicle.schemas.payload.input_payload import InputPayload
from kronicle.schemas.payload.response_payload import ResponsePayload
from kronicle.schemas.permissions.permission import PermStr
from kronicle.services.channel_service import ChannelService
from kronicle.services.core_service import CoreService
from kronicle.utils.str_utils import ensure_uuid4

writer_router = APIRouter(
    tags=["Input data"],
    dependencies=[
        Depends(require_auth),
        Depends(require_permission(PermStr.DATA_ACCESS)),
    ],
)

# --------------------------------------------------------------------------------------------------
# READ-ONLY ENDPOINTS
# --------------------------------------------------------------------------------------------------
writer_router.include_router(shared_read_router)


# --------------------------------------------------------------------------------------------------
# WRITE ENDPOINTS (append-only)
# --------------------------------------------------------------------------------------------------
writer_router.include_router(shared_writer_router)


@writer_router.post(
    "/zones/{zone_id}/channels",
    summary="Create a channel in a zone with optional rows",
    description=(
        "Creates a new channel inside the specified zone and inserts data rows.\n" "The channel must not already exist."
    ),
    response_model=ResponsePayload,
    dependencies=[
        Depends(
            require_permission_set(
                PermStr.CHANNEL_CREATE,
                PermStr.ROW_CREATE,
            )
        ),
    ],
)
async def create_channel_in_zone(
    zone_id: UUID,
    payload: InputPayload,
    core: CoreService = Depends(core_service),  # noqa: B008
    data_service: ChannelService = Depends(channel_service),  # noqa: B008
    strict: bool = Query(False, description="If true, abort on any validation error"),
):
    core.ensure_channel_in_zone(ensure_uuid4(payload.channel_id), zone_id)
    return await data_service.upsert_metadata_and_insert_rows(payload=payload, strict=strict)


@writer_router.post(
    "/channels/{channel_id}",
    summary="Update channel metadata and insert rows",
    description=("Updates metadata for an existing channel and inserts data rows.\n" "The channel must already exist."),
    response_model=ResponsePayload,
    dependencies=[
        Depends(
            require_permission_set(
                PermStr.CHANNEL_UPDATE,
                PermStr.ROW_CREATE,
            )
        )
    ],
)
async def update_channel_and_insert_rows(
    channel_id: UUID,
    payload: InputPayload,
    core: CoreService = Depends(core_service),  # noqa: B008
    data_service: ChannelService = Depends(channel_service),  # noqa: B008
    strict: bool = Query(False, description="If true, abort on any validation error"),
):
    if not core.get_core_channel(channel_id):
        raise HTTPException(
            status_code=404,
            detail=f"Channel {channel_id} not found in RBAC; sync may be needed",
        )
    payload.channel_id = channel_id
    return await data_service.upsert_metadata_and_insert_rows(payload, strict=strict)
