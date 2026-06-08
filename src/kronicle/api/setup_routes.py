# kronicle/api/setup_routes.py
from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Body, Depends

from kronicle.api.shared_read_routes import shared_read_router
from kronicle.api.shared_write_routes import shared_writer_router
from kronicle.auth.auth_middleware import require_auth
from kronicle.db.data.models.schema_registry import SchemaRegistry
from kronicle.deps.channel_deps import channel_service
from kronicle.deps.rbac_deps import rbac_service
from kronicle.errors.error_types import NotFoundError
from kronicle.schemas.filters.row_query_filter import RowQueryFilter
from kronicle.schemas.filters.row_request_filter import RowRequestFilter
from kronicle.schemas.payload.input_payload import InputPayload
from kronicle.schemas.payload.response_payload import ResponsePayload
from kronicle.schemas.rbac.input_zone_schemas import InputZone
from kronicle.schemas.rbac.safe_zone_schemas import OutputZone
from kronicle.services.channel_service import ChannelService
from kronicle.services.rbac_service import RbacService

"""
Admin/setup routes:
- Create, update, delete channels
- Clone channels
- Delete all rows for a channel
- list all channels with metadata and row counts
"""
setup_router = APIRouter(tags=["Setup data channels"], dependencies=[Depends(require_auth)])


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
        "Create a new channel with metadata and schema. Does not add rows, and fails if the channel already exists."
    ),
    response_model=ResponsePayload,
)
async def create_channel(
    payload: InputPayload,
    controller: ChannelService = Depends(channel_service),  # noqa: B008
):
    return await controller.create_channel(payload)


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
    controller: ChannelService = Depends(channel_service),  # noqa: B008
):
    return await controller.upsert_metadata(payload)


@setup_router.patch(
    "/channels/{channel_id}",
    summary="Partially update a channel",
    description="Update only a subset of metadata, tags, or schema for the specified channel.",
    response_model=ResponsePayload,
)
async def patch_channel(
    payload: InputPayload,
    controller: ChannelService = Depends(channel_service),  # noqa: B008
):
    return await controller.patch_metadata(payload)


@setup_router.post(
    "/channels/{channel_id}/clone",
    summary="Clone a channel",
    description="Creates a new channel by cloning an existing channel's schema and optionally metadata. "
    "Does not copy data rows nor name.",
    response_model=ResponsePayload,
)
async def clone_channel(
    payload: InputPayload,
    controller: ChannelService = Depends(channel_service),  # noqa: B008
):
    return await controller.clone_channel(payload)


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
)
async def delete_channel(
    channel_id: UUID,
    controller: ChannelService = Depends(channel_service),  # noqa: B008
):
    return await controller.delete_channel(channel_id)


@setup_router.delete(
    "/channels/{channel_id}/rows",
    summary="Delete all rows for a channel",
    description="Removes all data rows for the specified channel, while keeping its metadata intact.",
    response_model=ResponsePayload,
)
async def delete_channel_rows(
    channel_id: UUID,
    filter: Annotated[RowQueryFilter, Depends()],
    controller: ChannelService = Depends(channel_service),  # noqa: B008
):
    request_filter = RowRequestFilter.from_query(filter)
    return await controller.delete_rows_for_channel(channel_id, filter=request_filter)


@setup_router.post(
    "/channels/batch-delete",
    summary="Delete multiple channels",
    response_model=list[ResponsePayload],
)
async def batch_delete_channels(
    payload: dict = Body(..., examples=[{"channel_ids": ["uuid1", "uuid2"]}]),  # noqa
    controller: ChannelService = Depends(channel_service),  # noqa: B008
):
    return await controller.delete_channels(payload["channel_ids"])


@setup_router.get(
    "/schemas/column_types",
    summary="list the types available to describe the columns",
    description=("Retrieves every Python-like type that can be used to describe the type of a data column"),
    response_model=list[str],
)
async def get_column_types():
    return SchemaRegistry().allowed_types


# --------------------------------------------------------------------------------------------------
# Zone endpoints
# --------------------------------------------------------------------------------------------------


@setup_router.post(
    "/zones",
    summary="Create a zone",
    description="Creates a new zone for resource isolation and RBAC scoping.",
    response_model=OutputZone,
)
def create_zone(
    zone_in: InputZone,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.create_zone(name=zone_in.name, details=zone_in.details)


@setup_router.get(
    "/zones",
    summary="List all zones",
    description="Returns all zones.",
    response_model=list[OutputZone],
)
def list_zones(
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    zones = rbac.get_zones()
    return [OutputZone.from_db_zone(z) for z in zones]


@setup_router.get(
    "/zones/{zone_id}",
    summary="Get a zone by ID",
    description="Returns a single zone.",
    response_model=OutputZone,
)
def get_zone(
    zone_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    zone = rbac.get_zone(zone_id)
    if not zone:
        raise NotFoundError(f"Zone '{zone_id}' not found")
    return zone


@setup_router.delete(
    "/zones/{zone_id}",
    summary="Delete a zone",
    description="Deletes a zone and its hierarchy links.",
    response_model=OutputZone,
)
def delete_zone(
    zone_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    zone = rbac.delete_zone(zone_id)
    if not zone:
        raise NotFoundError(f"Zone '{zone_id}' not found")
    return zone


# --------------------------------------------------------------------------------------------------
# Sync: reconcile data channels with core RBAC records
# --------------------------------------------------------------------------------------------------


@setup_router.post(
    "/core/sync",
    summary="Sync data channels to CoreChannels",
    description=(
        "Scans all ChannelResources in the data DB and creates missing CoreChannel records "
        "in the core RBAC schema. Also ensures a default zone exists."
    ),
    response_model=dict,
)
async def sync_core_channels(
    controller: ChannelService = Depends(channel_service),  # noqa: B008
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    data_channels = await controller.fetch_all_metadata()
    channel_ids = [c.channel_id for c in data_channels]

    default_zone = rbac.ensure_default_zone()
    created = rbac.sync_core_channels(channel_ids, default_zone_id=default_zone.id)

    return {
        "detail": f"Synced {len(channel_ids)} data channels",
        "total_data_channels": len(channel_ids),
        "created_core_channels": len(created),
        "default_zone_id": str(default_zone.id),
    }
