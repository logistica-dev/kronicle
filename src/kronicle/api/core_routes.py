# kronicle/api/core_routes.py
from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query

from kronicle.auth.auth_middleware import require_auth, require_permission
from kronicle.deps.channel_deps import channel_service
from kronicle.deps.rbac_deps import core_service
from kronicle.errors.error_types import NotFoundError
from kronicle.schemas.core.input_ressource_schema import (
    InputCoreChannel,
    InputCoreChannelPatch,
    InputZone,
    InputZonePatch,
)
from kronicle.schemas.core.safe_ressource_schema import OutputCoreChannel, OutputZone
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
    response_model=OutputZone | list[OutputZone] | None,
    dependencies=[Depends(require_permission(PermStr.ZONE_READ))],
)
def list_zones(
    name: str | None = Query(None, description="Optional name to filter by"),
    core: CoreService = Depends(core_service),  # noqa: B008
):
    if name:
        return core.get_zone_by_name(name)
    zones = core.get_zones()
    return zones


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
    response_model=OutputCoreChannel | list[OutputCoreChannel] | None,
    dependencies=[Depends(require_permission(PermStr.CHANNEL_READ))],
)
def list_channels(
    name: str | None = Query(None, description="Optional name to filter by"),
    core: CoreService = Depends(core_service),  # noqa: B008
):
    if name:
        return core.get_core_channel_by_name(name)
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
        zone_id=channel_in.zone.id if channel_in.zone else None,
    )


@core_router.delete(
    "/channels/{channel_id}",
    summary="Delete a core channel",
    description="Deletes a CoreChannel record. Does not affect the data channel.",
    response_model=OutputCoreChannel,
    dependencies=[Depends(require_permission(PermStr.CHANNEL_DELETE))],
)
def delete_core_channel(
    channel_id: UUID,
    core: CoreService = Depends(core_service),  # noqa: B008
):
    deleted = core.delete_core_channel(channel_id)
    if not deleted:
        raise NotFoundError(f"CoreChannel '{channel_id}' not found")
    return deleted


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
    response_model=dict[str, str | int],
    dependencies=[Depends(require_permission(PermStr.CHANNEL_SYNC))],
)
async def sync_core_channels(
    data_service: ChannelService = Depends(channel_service),  # noqa: B008
    core: CoreService = Depends(core_service),  # noqa: B008
):
    data_channels = await data_service.fetch_all_metadata()
    channels_info = [InputCoreChannel(id=c.id, name=c.name) for c in data_channels]

    default_zone = core.ensure_default_zone()
    created = core.sync_core_channels(channels_info, default_zone_id=default_zone.id)

    return {
        "detail": f"Synced {len(channels_info)} data channels",
        "total_data_channels": len(channels_info),
        "created_core_channels": len(created),
        "default_zone_id": uuid_to_str(default_zone.id),
    }
