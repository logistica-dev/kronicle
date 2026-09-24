# tests/unit/api/test_core_routes.py
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from kronicle.api.core_routes import (
    create_zone,
    delete_core_channel,
    delete_zone,
    get_core_channel,
    get_zone,
    list_channels,
    list_zone_channels,
    list_zones,
    patch_core_channel,
    patch_zone,
    sync_core_channels,
)
from kronicle.errors.error_types import NotFoundError
from kronicle.schemas.core.input_ressource_schema import InputCoreChannelPatch, InputZone, InputZonePatch
from kronicle.utils.str_utils import uuid_to_str

ZONE_NAME = "zone_test_a"
ZONE_ID = UUID("303fa7e1-4533-4ce1-8f89-c0c9c5466af6")
CHAN_ID = UUID("fafafa78-4533-4ce1-8f89-c0c9c5466af6")


def _zone(name=ZONE_NAME):
    return InputZone(name=name)


class TestCreateZone:
    def test_creates_zone(self):
        core = MagicMock()
        core.create_zone.return_value = "created"
        zone_in = _zone()
        assert create_zone(zone_in=zone_in, core=core) == "created"
        core.create_zone.assert_called_once_with(name=ZONE_NAME, details=None)


class TestListZones:
    def test_filters_by_name(self):
        core = MagicMock()
        core.get_zone_by_name.return_value = "by-name"
        assert list_zones(name=ZONE_NAME, core=core) == "by-name"
        core.get_zone_by_name.assert_called_once_with(ZONE_NAME)

    def test_lists_all(self):
        core = MagicMock()
        core.get_zones.return_value = ["z1", "z2"]
        assert list_zones(name=None, core=core) == ["z1", "z2"]


class TestGetZone:
    def test_returns_zone(self):
        core = MagicMock()
        core.get_zone.return_value = "zone"
        assert get_zone(zone_id=ZONE_ID, core=core) == "zone"

    def test_raises_not_found(self):
        core = MagicMock()
        core.get_zone.return_value = None
        with pytest.raises(NotFoundError, match=str(ZONE_ID)):
            get_zone(zone_id=ZONE_ID, core=core)


class TestPatchZone:
    def test_patches_zone(self):
        core = MagicMock()
        core.patch_zone.return_value = "patched"
        assert patch_zone(zone_id=ZONE_ID, zone_in=InputZonePatch(), core=core) == "patched"
        core.patch_zone.assert_called_once_with(ZONE_ID, name=None, details=None)


class TestDeleteZone:
    def test_deletes_zone(self):
        core = MagicMock()
        core.delete_zone.return_value = "deleted"
        assert delete_zone(zone_id=ZONE_ID, core=core) == "deleted"
        core.delete_zone.assert_called_once_with(ZONE_ID)


class TestListZoneChannels:
    def test_lists_channels_for_zone(self):
        core = MagicMock()
        core.get_core_channels.return_value = ["chan"]
        assert list_zone_channels(zone_id=ZONE_ID, core=core) == ["chan"]
        core.get_core_channels.assert_called_once_with(zone_id=ZONE_ID)


class TestListChannels:
    def test_filters_by_name(self):
        core = MagicMock()
        core.get_core_channel_by_name.return_value = "by-name"
        assert list_channels(name="my_channel", core=core) == "by-name"
        core.get_core_channel_by_name.assert_called_once_with("my_channel")

    def test_lists_all(self):
        core = MagicMock()
        core.get_core_channels.return_value = ["c1"]
        assert list_channels(name=None, core=core) == ["c1"]


class TestGetCoreChannel:
    def test_returns_channel(self):
        core = MagicMock()
        core.get_core_channel.return_value = "chan"
        assert get_core_channel(channel_id=CHAN_ID, core=core) == "chan"

    def test_raises_not_found(self):
        core = MagicMock()
        core.get_core_channel.return_value = None
        with pytest.raises(NotFoundError, match=str(CHAN_ID)):
            get_core_channel(channel_id=CHAN_ID, core=core)


class TestPatchCoreChannel:
    def test_patches_without_zone(self):
        core = MagicMock()
        core.patch_core_channel.return_value = "patched"
        result = patch_core_channel(channel_id=CHAN_ID, channel_in=InputCoreChannelPatch(), core=core)
        core.patch_core_channel.assert_called_once_with(CHAN_ID, name=None, details=None, zone_id=None)
        assert result == "patched"

    def test_patches_with_zone(self):
        core = MagicMock()
        core.patch_core_channel.return_value = "patched"
        zone = _zone("new_zone_a")
        result = patch_core_channel(channel_id=CHAN_ID, channel_in=InputCoreChannelPatch(zone=zone), core=core)
        core.patch_core_channel.assert_called_once_with(CHAN_ID, name=None, details=None, zone_id=zone.id)
        assert result == "patched"


class TestDeleteCoreChannel:
    def test_deletes_channel(self):
        core = MagicMock()
        core.delete_core_channel.return_value = "deleted"
        assert delete_core_channel(channel_id=CHAN_ID, core=core) == "deleted"

    def test_raises_not_found(self):
        core = MagicMock()
        core.delete_core_channel.return_value = None
        with pytest.raises(NotFoundError, match=str(CHAN_ID)):
            delete_core_channel(channel_id=CHAN_ID, core=core)


class TestSyncCoreChannels:
    @pytest.mark.asyncio
    async def test_syncs_channels(self):
        data_service = AsyncMock()
        channel_id_1, channel_id_2, zone_id = uuid4(), uuid4(), uuid4()
        data_channels = [
            SimpleNamespace(id=channel_id_1, name="chan_1"),
            SimpleNamespace(id=channel_id_2, name="chan_2"),
        ]
        data_service.fetch_all_metadata.return_value = data_channels
        core = MagicMock()
        core.ensure_default_zone.return_value = SimpleNamespace(id=zone_id)
        core.create_missing_core_channels.return_value = [channel_id_2]
        core.delete_orphan_core_channels.return_value = [uuid4()]

        result = await sync_core_channels(data_service=data_service, core=core)

        assert result["total_data_channels"] == 2
        assert result["created_core_channels"] == 1
        assert result["deleted_core_channels"] == 1
        assert result["default_zone_id"] == uuid_to_str(zone_id)
        assert "Synced 2 data channels" in result["detail"]
        assert core.create_missing_core_channels.call_count == 1
        core.delete_orphan_core_channels.assert_called_once_with({channel_id_1, channel_id_2})
