# tests/unit/api/test_write_routes.py
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from kronicle.api.write_routes import update_channel_and_insert_rows
from kronicle.schemas.payload.input_payload import InputPayload


class TestUpdateChannelAndInsertRows:
    @pytest.mark.asyncio
    async def test_raises_404_when_channel_missing_from_core(self):
        core = MagicMock()
        core.get_core_channel.return_value = None
        data_service = AsyncMock()
        payload = InputPayload(name="some_channel")
        channel_id = uuid4()

        with pytest.raises(HTTPException) as exc_info:
            await update_channel_and_insert_rows(
                channel_id=channel_id,
                payload=payload,
                core=core,
                data_service=data_service,
                strict=False,
            )

        assert exc_info.value.status_code == 404
        assert str(channel_id) in exc_info.value.detail
        core.get_core_channel.assert_called_once_with(channel_id)
        data_service.upsert_metadata_and_insert_rows.assert_not_called()

    @pytest.mark.asyncio
    async def test_binds_payload_id_and_inserts_rows(self):
        core = MagicMock()
        core.get_core_channel.return_value = MagicMock()
        data_service = AsyncMock()
        data_service.upsert_metadata_and_insert_rows.return_value = {"status": "ok"}
        channel_id = uuid4()
        payload = InputPayload(name="some_channel")
        assert payload.id is None

        result = await update_channel_and_insert_rows(
            channel_id=channel_id,
            payload=payload,
            core=core,
            data_service=data_service,
            strict=True,
        )

        assert result == {"status": "ok"}
        # The path channel_id wins: it is forced onto the payload before the upsert.
        assert payload.id == channel_id
        data_service.upsert_metadata_and_insert_rows.assert_awaited_once_with(payload, strict=True)
