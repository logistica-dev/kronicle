# kronicle/api/core_routes.py
from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends

from kronicle.auth.auth_middleware import require_auth, require_permission
from kronicle.deps.channel_deps import channel_service
from kronicle.deps.rbac_deps import core_service
from kronicle.errors.error_types import NotFoundError
from kronicle.schemas.core.input_core_channel_schemas import InputCoreChannelPatch
from kronicle.schemas.core.input_zone_schemas import InputZone, InputZonePatch
from kronicle.schemas.core.safe_core_channel_schemas import OutputCoreChannel
from kronicle.schemas.core.safe_zone_schemas import OutputZone
from kronicle.schemas.permissions.permission import PermStr
from kronicle.services.channel_service import ChannelService
from kronicle.services.core_service import CoreService
from kronicle.utils.str_utils import uuid_to_str

core_router = APIRouter(tags=["Core"], dependencies=[Depends(require_auth)])


# --------------------------------------------------------------------------------------------------
# Zones
# --------------------------------------------------------------------------------------------------


@core_router.post(
    "/zones",
    summary="Create a zone",
    description="Creates a new zone for resource isolation and RBAC scoping.",
    response_model=OutputZone,
    dependencies=[Depends(require_permission(PermStr.ZONE_CREATE))],
)
def create_zone(
    zone_in: InputZone,
    core: CoreService = Depends(core_service),  # noqa: B008
):
    return core.create_zone(name=zone_in.name, details=zone_in.details)


@core_router.get(
    "/zones",
    summary="List all zones",
    description="Returns all zones.",
    response_model=list[OutputZone],
    dependencies=[Depends(require_permission(PermStr.ZONE_READ))],
)
def list_zones(
    core: CoreService = Depends(core_service),  # noqa: B008
):
    zones = core.get_zones()
    return [OutputZone.from_db_zone(z) for z in zones]


@core_router.get(
    "/zones/{zone_id}",
    summary="Get a zone by ID",
    description="Returns a single zone.",
    response_model=OutputZone,
    dependencies=[Depends(require_permission(PermStr.ZONE_READ))],
)
def get_zone(
    zone_id: UUID,
    core: CoreService = Depends(core_service),  # noqa: B008
):
    zone = core.get_zone(zone_id)
    if not zone:
        raise NotFoundError(f"Zone '{zone_id}' not found")
    return zone


@core_router.patch(
    "/zones/{zone_id}",
    summary="Patch a zone",
    description="Partially update a zone's name or details.",
    response_model=OutputZone,
    dependencies=[Depends(require_permission(PermStr.ZONE_UPDATE))],
)
def patch_zone(
    zone_id: UUID,
    zone_in: InputZonePatch,
    core: CoreService = Depends(core_service),  # noqa: B008
):
    return core.patch_zone(zone_id, name=zone_in.name, details=zone_in.details)


@core_router.delete(
    "/zones/{zone_id}",
    summary="Delete a zone",
    description="Deletes a zone and its hierarchy links.",
    response_model=OutputZone,
    dependencies=[Depends(require_permission(PermStr.ZONE_DELETE))],
)
def delete_zone(
    zone_id: UUID,
    core: CoreService = Depends(core_service),  # noqa: B008
):
    zone = core.delete_zone(zone_id)
    return zone


# --------------------------------------------------------------------------------------------------
# Core Channels
# --------------------------------------------------------------------------------------------------


@core_router.get(
    "/zones/{zone_id}/channels",
    summary="List all core channels in a zone",
    description="Returns all CoreChannels belonging to the specified zone.",
    response_model=list[OutputCoreChannel],
    dependencies=[Depends(require_permission(PermStr.CHANNEL_READ))],
)
def list_zone_channels(
    zone_id: UUID,
    core: CoreService = Depends(core_service),  # noqa: B008
):
    return core.get_core_channels(zone_id=zone_id)


@core_router.get(
    "/channels",
    summary="List all core channels",
    description="Returns all CoreChannels.",
    response_model=list[OutputCoreChannel],
    dependencies=[Depends(require_permission(PermStr.CHANNEL_READ))],
)
def list_channels(
    core: CoreService = Depends(core_service),  # noqa: B008
):
    return core.get_core_channels()


@core_router.get(
    "/channels/{channel_id}",
    summary="Get a core channel by ID",
    description="Returns a single CoreChannel.",
    response_model=OutputCoreChannel,
    dependencies=[Depends(require_permission(PermStr.CHANNEL_READ))],
)
def get_core_channel(
    channel_id: UUID,
    core: CoreService = Depends(core_service),  # noqa: B008
):
    channel = core.get_core_channel(channel_id)
    if not channel:
        raise NotFoundError(f"CoreChannel '{channel_id}' not found")
    return channel


@core_router.patch(
    "/channels/{channel_id}",
    summary="Patch a core channel",
    description="Partially update a CoreChannel's name, details, or zone_id.",
    response_model=OutputCoreChannel,
    dependencies=[Depends(require_permission(PermStr.CHANNEL_UPDATE))],
)
def patch_core_channel(
    channel_id: UUID,
    channel_in: InputCoreChannelPatch,
    core: CoreService = Depends(core_service),  # noqa: B008
):
    return core.patch_core_channel(
        channel_id,
        name=channel_in.name,
        details=channel_in.details,
        zone_id=channel_in.zone_id,
    )


# --------------------------------------------------------------------------------------------------
# Sync: reconcile data channels with core RBAC records
# --------------------------------------------------------------------------------------------------


@core_router.post(
    "/sync",
    summary="Sync data channels to CoreChannels",
    description=(
        "Scans all ChannelResources in the data DB and creates missing CoreChannel records "
        "in the core RBAC schema. Also ensures a default zone exists."
    ),
    response_model=dict,
    dependencies=[Depends(require_permission(PermStr.CHANNEL_SYNC))],
)
async def sync_core_channels(
    data_service: ChannelService = Depends(channel_service),  # noqa: B008
    core: CoreService = Depends(core_service),  # noqa: B008
):
    data_channels = await data_service.fetch_all_metadata()
    channel_ids = [c.channel_id for c in data_channels]

    default_zone = core.ensure_default_zone()
    created = core.sync_core_channels(channel_ids, default_zone_id=default_zone.id)

    return {
        "detail": f"Synced {len(channel_ids)} data channels",
        "total_data_channels": len(channel_ids),
        "created_core_channels": len(created),
        "default_zone_id": uuid_to_str(default_zone.id),
    }
