# kronicle/api/write_routes.py
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from kronicle.auth.auth_middleware import require_auth
from kronicle.deps.channel_deps import channel_service
from kronicle.schemas.payload.input_payload import InputPayload
from kronicle.schemas.payload.response_payload import ResponsePayload
from kronicle.services.channel_service import ChannelService

"""
Routes available to users with write permissions.
These endpoints allow safe retrieval of channel metadata and stored data but mainly adding rows to
existing (or new) channel.
"""
shared_writer_router = APIRouter(dependencies=[Depends(require_auth)])


# --------------------------------------------------------------------------------------------------
# WRITE ENDPOINTS (append-only)
# --------------------------------------------------------------------------------------------------


@shared_writer_router.post(
    "/channels/{channel_id}/rows",
    summary="Insert rows for a  channel",
    description="Append-only operation: insert new rows for an existing channel. Does not modify metadata or schema.",
    response_model=ResponsePayload,
)
async def insert_rows(
    channel_id: UUID,
    payload: InputPayload,
    controller: ChannelService = Depends(channel_service),  # noqa: B008
    strict: bool = Query(False, description="If true, abort on any validation error"),
):
    payload.channel_id = channel_id  # path param overrides any payload channel_id
    return await controller.insert_channel_rows(payload, strict=strict)
