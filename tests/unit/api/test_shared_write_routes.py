# tests/unit/api/test_shared_write_routes.py
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException

from kronicle.api.shared_write_routes import create_channel_in_zone, insert_rows, patch_channel
from kronicle.db.data.models.channel_schema import ChannelSchema
from kronicle.schemas.payload.response_payload import ResponsePayload


def _response_payload(**op_details):
    return ResponsePayload(
        id=uuid4(),
        channel_schema=ChannelSchema.from_user_json({"time": "time"}),
        op_details=op_details,
    )


class TestCreateChannelInZone:
    @patch("kronicle.api.shared_write_routes.InputCoreChannel.from_payload")
    @pytest.mark.asyncio
    async def test_ensures_channel_and_creates(self, mock_from_payload):
        core = MagicMock()
        data_service = AsyncMock()
        data_service.create_channel.return_value = "created"
        zone_id = uuid4()
        payload = MagicMock()
        expected_core = MagicMock()
        mock_from_payload.return_value = expected_core
        result = await create_channel_in_zone(zone_id=zone_id, payload=payload, data_service=data_service, core=core)
        mock_from_payload.assert_called_once_with(payload)
        core.ensure_channel_in_zone.assert_called_once_with(expected_core, zone_id)
        data_service.create_channel.assert_awaited_once_with(payload)
        assert result == "created"


class TestPatchChannel:
    @pytest.mark.asyncio
    async def test_patches_name_and_metadata(self):
        core = MagicMock()
        core.get_core_channel.return_value = {"id": "chan"}
        data_service = AsyncMock()
        data_service.patch_metadata.return_value = "patched"
        channel_id = uuid4()
        payload = MagicMock()
        payload.name = "new_name"
        result = await patch_channel(channel_id=channel_id, payload=payload, data_service=data_service, core=core)
        assert payload.id == channel_id
        core.patch_core_channel.assert_called_once_with(channel_id, name="new_name")
        data_service.patch_metadata.assert_awaited_once_with(payload)
        assert result == "patched"

    @pytest.mark.asyncio
    async def test_no_patch_core_when_no_name(self):
        core = MagicMock()
        core.get_core_channel.return_value = {"id": "chan"}
        data_service = AsyncMock()
        payload = MagicMock()
        payload.name = None
        await patch_channel(channel_id=uuid4(), payload=payload, data_service=data_service, core=core)
        core.patch_core_channel.assert_not_called()

    @pytest.mark.asyncio
    async def test_raises_404_when_core_channel_missing(self):
        core = MagicMock()
        core.get_core_channel.return_value = None
        data_service = AsyncMock()
        payload = MagicMock()
        with pytest.raises(HTTPException) as exc_info:
            await patch_channel(channel_id=uuid4(), payload=payload, data_service=data_service, core=core)
        assert exc_info.value.status_code == 404
        data_service.patch_metadata.assert_not_awaited()


class TestInsertRows:
    @pytest.mark.asyncio
    async def test_inserts_and_adds_read_policies_for_users(self):
        data_service = AsyncMock()
        data_service.insert_channel_rows.side_effect = lambda payload, strict: _response_payload(
            inserted_row_ids=[1, 2, 3]
        )
        rbac = MagicMock()
        payload = MagicMock()
        payload.read_users = [uuid4()]
        payload.read_groups = None
        channel_id = uuid4()
        result = await insert_rows(channel_id=channel_id, payload=payload, data_service=data_service, rbac=rbac)
        assert payload.id == channel_id
        rbac.add_row_read_policies.assert_called_once()
        assert "inserted_row_ids" not in result.op_details

    @pytest.mark.asyncio
    async def test_inserts_and_adds_read_policies_for_groups(self):
        data_service = AsyncMock()
        data_service.insert_channel_rows.side_effect = lambda payload, strict: _response_payload(inserted_row_ids=[4])
        rbac = MagicMock()
        payload = MagicMock()
        payload.read_users = None
        payload.read_groups = [uuid4()]
        result = await insert_rows(channel_id=uuid4(), payload=payload, data_service=data_service, rbac=rbac)
        rbac.add_row_read_policies.assert_called_once()
        assert "inserted_row_ids" not in result.op_details

    @pytest.mark.asyncio
    async def test_no_readers_skips_policies(self):
        data_service = AsyncMock()
        data_service.insert_channel_rows.side_effect = lambda payload, strict: _response_payload(inserted_row_ids=[1])
        rbac = MagicMock()
        payload = MagicMock()
        payload.read_users = None
        payload.read_groups = None
        result = await insert_rows(channel_id=uuid4(), payload=payload, data_service=data_service, rbac=rbac)
        rbac.add_row_read_policies.assert_not_called()
        assert result.op_details["inserted_row_ids"] == [1]

    @pytest.mark.asyncio
    async def test_no_inserted_rows_skips_policies(self):
        data_service = AsyncMock()
        data_service.insert_channel_rows.side_effect = lambda payload, strict: _response_payload()
        rbac = MagicMock()
        payload = MagicMock()
        payload.read_users = [uuid4()]
        payload.read_groups = None
        await insert_rows(channel_id=uuid4(), payload=payload, data_service=data_service, rbac=rbac)
        rbac.add_row_read_policies.assert_not_called()
