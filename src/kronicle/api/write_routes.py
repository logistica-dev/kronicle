# kronicle/api/write_routes.py

from fastapi import APIRouter, Depends, Query

from kronicle.api.shared_read_routes import shared_read_router
from kronicle.api.shared_write_routes import shared_writer_router
from kronicle.auth.auth_middleware import require_auth
from kronicle.deps.channel_deps import channel_service
from kronicle.schemas.payload.input_payload import InputPayload
from kronicle.schemas.payload.response_payload import ResponsePayload
from kronicle.services.channel_service import ChannelService

writer_router = APIRouter(tags=["Input data"], dependencies=[Depends(require_auth)])

# --------------------------------------------------------------------------------------------------
# READ-ONLY ENDPOINTS
# --------------------------------------------------------------------------------------------------
writer_router.include_router(shared_read_router)


# --------------------------------------------------------------------------------------------------
# WRITE ENDPOINTS (append-only)
# --------------------------------------------------------------------------------------------------
writer_router.include_router(shared_writer_router)


@writer_router.post(
    "/channels",
    summary="Upsert metadata and insert rows",
    description="Append-only operation: creates new metadata if missing and inserts channel data rows",
    response_model=ResponsePayload,
)
async def upsert_metadata_and_rows(
    payload: InputPayload,
    controller: ChannelService = Depends(channel_service),  # noqa: B008
    strict: bool = Query(False, description="If true, abort on any validation error"),
):
    return await controller.upsert_metadata_and_insert_rows(payload, strict)
