# kronicle/api/write_routes.py

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from kronicle.api.shared_read_routes import shared_read_router
from kronicle.api.shared_write_routes import shared_writer_router
from kronicle.auth.auth_middleware import require_auth, require_permission, require_permission_set
from kronicle.deps.channel_deps import channel_service
from kronicle.schemas.payload.input_payload import InputPayload
from kronicle.schemas.payload.response_payload import ResponsePayload
from kronicle.schemas.permissions.permission import Permission, PermissionAction, PermissionTarget
from kronicle.services.channel_service import ChannelService
from kronicle.services.rbac_service import RbacService
from kronicle.utils.str_utils import ensure_uuid4


async def validate_zone_channel_binding(zone_id: UUID, request: Request) -> None:
    """FastAPI dependency: verify or create the CoreChannel-zone association.

    - If a CoreChannel already exists for the body's channel_id, checks its zone_id matches.
    - If it doesn't exist, creates a new CoreChannel in the given zone.
    - Raises 400 on zone mismatch.
    """
    body = await request.json()
    channel_id = ensure_uuid4(body.get("channel_id", ""))
    rbac: RbacService = request.app.state.rbac_service

    existing = rbac.get_core_channel(channel_id)
    if existing:
        if existing.zone_id != zone_id:
            raise HTTPException(
                status_code=400,
                detail=f"Channel {channel_id} belongs to zone {existing.zone_id}, not {zone_id}",
            )
    else:
        rbac.create_core_channel(channel_id, zone_id)


writer_router = APIRouter(
    tags=["Input data"],
    dependencies=[
        Depends(require_auth),
        Depends(require_permission(Permission(PermissionTarget.DATA, PermissionAction.ACCESS))),
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
        Depends(validate_zone_channel_binding),
        Depends(
            require_permission_set(
                Permission(PermissionTarget.CHANNEL, PermissionAction.CREATE),
                Permission(PermissionTarget.ROW, PermissionAction.CREATE),
            )
        ),
    ],
)
async def create_channel_in_zone(
    zone_id: UUID,
    payload: InputPayload,
    data_service: ChannelService = Depends(channel_service),  # noqa: B008
    strict: bool = Query(False, description="If true, abort on any validation error"),
):
    return await data_service.upsert_metadata_and_insert_rows(payload=payload, strict=strict)


@writer_router.post(
    "/channels/{channel_id}",
    summary="Update channel metadata and insert rows",
    description=("Updates metadata for an existing channel and inserts data rows.\n" "The channel must already exist."),
    response_model=ResponsePayload,
    dependencies=[
        Depends(
            require_permission_set(
                Permission(PermissionTarget.CHANNEL, PermissionAction.UPDATE),
                Permission(PermissionTarget.ROW, PermissionAction.CREATE),
            )
        )
    ],
)
async def update_channel_and_insert_rows(
    channel_id: UUID,
    zone_id: UUID,
    payload: InputPayload,
    data_service: ChannelService = Depends(channel_service),  # noqa: B008
    strict: bool = Query(False, description="If true, abort on any validation error"),
):
    payload.channel_id = channel_id
    return await data_service.upsert_metadata_and_insert_rows(payload, strict=strict)
