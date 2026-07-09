# tests/unit/services/test_core_service.py
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from kronicle.db.core.models.core_channel import CoreChannel
from kronicle.db.core.models.core_zone import CoreZone
from kronicle.db.rbac.rbac_db_session import RbacDbSession
from kronicle.errors.error_types import BadRequestError, ConflictError, NotFoundError
from kronicle.repo.core.core_channel_repo import CoreChannelRepository
from kronicle.repo.core.core_zone_repo import CoreZoneRepository
from kronicle.schemas.core.input_ressource_schema import InputCoreChannel
from kronicle.schemas.core.safe_ressource_schema import OutputZone
from kronicle.services.core_service import CoreService


@pytest.fixture
def mock_db_session():
    session = MagicMock()
    session.add = MagicMock()
    session.flush = MagicMock()
    session.delete = MagicMock()
    session.refresh = MagicMock()
    return session


@pytest.fixture
def mock_db(mock_db_session):
    db = MagicMock(spec=RbacDbSession)
    db.get_db.return_value.__enter__.return_value = mock_db_session
    db.transaction.return_value.__enter__.return_value = mock_db_session
    return db


@pytest.fixture
def mock_zone_repo():
    return MagicMock(spec=CoreZoneRepository)


@pytest.fixture
def mock_channel_repo():
    return MagicMock(spec=CoreChannelRepository)


@pytest.fixture
def service(mock_db, mock_zone_repo, mock_channel_repo):
    with (
        patch("kronicle.services.core_service.CoreChannelRepository", return_value=mock_channel_repo),
        patch("kronicle.services.core_service.CoreZoneRepository", return_value=mock_zone_repo),
        patch("kronicle.services.core_service.ZoneHierarchyRepository"),
        patch("kronicle.services.core_service.HierarchyEngine"),
        patch("kronicle.services.core_service.HierarchyService"),
    ):
        svc = CoreService(core_db_session=mock_db)
    return svc


def make_zone(id=None, name="test-zone", details=None):
    zone = MagicMock(spec=CoreZone)
    zone.id = id or uuid4()
    zone.name = name
    zone.details = details or {}
    return zone


def make_channel(id=None, name="test-channel", zone_id=None):
    channel = MagicMock(spec=CoreChannel)
    channel.id = id or uuid4()
    channel.name = name
    channel.details = {}
    channel.zone_id = zone_id
    return channel


# --------------------------------------------------------------------------------------
# Zones
# --------------------------------------------------------------------------------------


class TestGetZones:
    def test_returns_all_zones(self, service, mock_db, mock_db_session, mock_zone_repo):
        zones = [make_zone(), make_zone()]
        mock_zone_repo.fetch_all.return_value = zones

        with patch("kronicle.services.core_service.OutputZone.from_db") as mock_from:
            mock_from.side_effect = lambda z: MagicMock(id=z.id, name=z.name)
            result = service.get_zones()

        assert len(result) == 2
        mock_db.get_db.assert_called_once()
        mock_zone_repo.fetch_all.assert_called_once_with(mock_db_session)


class TestCreateZone:
    def test_creates_zone_successfully(self, service, mock_db, mock_db_session, mock_zone_repo):
        mock_zone_repo.get_by_name.return_value = None

        with patch("kronicle.services.core_service.OutputZone.from_db") as mock_from:
            mock_from.return_value = MagicMock(spec=OutputZone)
            result = service.create_zone("new-zone", {"key": "val"})

        assert result is not None
        mock_zone_repo.get_by_name.assert_called_once_with(mock_db_session, name="new-zone")
        mock_db_session.add.assert_called_once()
        mock_db_session.flush.assert_called_once()

    def test_raises_if_zone_exists(self, service, mock_db, mock_db_session, mock_zone_repo):
        mock_zone_repo.get_by_name.return_value = make_zone()

        with pytest.raises(BadRequestError, match="already exists"):
            service.create_zone("existing-zone")


class TestGetZone:
    def test_returns_zone(self, service, mock_db, mock_db_session, mock_zone_repo):
        zone = make_zone()
        mock_zone_repo.get_by_id.return_value = zone

        with patch("kronicle.services.core_service.OutputZone.from_db") as mock_from:
            mock_from.return_value = MagicMock(spec=OutputZone, id=zone.id)
            result = service.get_zone(zone.id)

        assert result is not None
        mock_zone_repo.get_by_id.assert_called_once_with(mock_db_session, id=zone.id)

    def test_returns_none_if_not_found(self, service, mock_db, mock_db_session, mock_zone_repo):
        mock_zone_repo.get_by_id.return_value = None
        result = service.get_zone(uuid4())
        assert result is None


class TestDeleteZone:
    def test_deletes_zone(self, service, mock_db, mock_db_session, mock_zone_repo):
        zone = make_zone()
        mock_zone_repo.get_by_id.return_value = zone

        with patch("kronicle.services.core_service.OutputZone.from_db") as mock_from:
            mock_from.return_value = MagicMock(spec=OutputZone, id=zone.id)
            result = service.delete_zone(zone.id)

        assert result is not None
        mock_db_session.delete.assert_called_once_with(zone)
        mock_db_session.flush.assert_called_once()

    def test_raises_if_not_found(self, service, mock_db, mock_db_session, mock_zone_repo):
        mock_zone_repo.get_by_id.return_value = None
        with pytest.raises(NotFoundError, match="not found"):
            service.delete_zone(uuid4())


class TestPatchZone:
    def test_updates_name(self, service, mock_db, mock_db_session, mock_zone_repo):
        zone = make_zone(name="old-name")
        mock_zone_repo.get_by_id.return_value = zone

        with patch("kronicle.services.core_service.OutputZone.from_db") as mock_from:
            mock_from.return_value = MagicMock(spec=OutputZone)
            result = service.patch_zone(zone.id, name="new-name")

        assert result is not None
        assert zone.name == "new-name"
        mock_db_session.flush.assert_called_once()
        mock_db_session.refresh.assert_called_once_with(zone)

    def test_updates_details(self, service, mock_db, mock_db_session, mock_zone_repo):
        zone = make_zone(details={"old": "val"})
        mock_zone_repo.get_by_id.return_value = zone

        with patch("kronicle.services.core_service.OutputZone.from_db") as mock_from:
            mock_from.return_value = MagicMock(spec=OutputZone)
            result = service.patch_zone(zone.id, details={"new": "val"})

        assert result is not None
        assert zone.details == {"new": "val"}

    def test_raises_if_not_found(self, service, mock_db, mock_db_session, mock_zone_repo):
        mock_zone_repo.get_by_id.return_value = None
        with pytest.raises(NotFoundError, match="not found"):
            service.patch_zone(uuid4(), name="new-name")


# --------------------------------------------------------------------------------------
# Core Channels
# --------------------------------------------------------------------------------------


class TestListCoreChannelIds:
    def test_returns_set_of_ids(self, service, mock_db, mock_db_session, mock_channel_repo):
        channels = [make_channel(), make_channel()]
        mock_channel_repo.fetch_all.return_value = channels

        result = service.list_core_channel_ids()

        assert result == {channels[0].id, channels[1].id}
        mock_channel_repo.fetch_all.assert_called_once_with(mock_db_session)


class TestSyncCoreChannels:
    def test_creates_missing_channels(self, service, mock_db, mock_db_session, mock_channel_repo):
        existing_ids = {uuid4(), uuid4()}
        mock_channel_repo.fetch_all.return_value = [MagicMock(id=eid) for eid in existing_ids]

        new_ids = [uuid4(), uuid4()]
        channels = [InputCoreChannel(id=eid) for eid in existing_ids] + [InputCoreChannel(id=nid) for nid in new_ids]

        result = service.sync_core_channels(channels)

        assert result == new_ids
        assert mock_db_session.add.call_count == 2

    def test_no_missing_channels_returns_empty(self, service, mock_db, mock_db_session, mock_channel_repo):
        existing_ids = {uuid4(), uuid4()}
        mock_channel_repo.fetch_all.return_value = [MagicMock(id=eid) for eid in existing_ids]

        channels = [InputCoreChannel(id=eid) for eid in existing_ids]
        result = service.sync_core_channels(channels)

        assert result == []

    def test_creates_with_default_zone(self, service, mock_db, mock_db_session, mock_channel_repo, mock_zone_repo):
        zone_id = uuid4()
        mock_channel_repo.fetch_all.return_value = []

        new_id = uuid4()
        result = service.sync_core_channels([InputCoreChannel(id=new_id)], default_zone_id=zone_id)

        assert result == [new_id]
        mock_db_session.add.assert_called_once()
        added = mock_db_session.add.call_args[0][0]
        assert added.zone_id == zone_id


class TestEnsureDefaultZone:
    def test_returns_existing_zone(self, service, mock_db, mock_db_session, mock_zone_repo):
        zone = make_zone(name="default")
        mock_zone_repo.get_by_name.return_value = zone

        result = service.ensure_default_zone()

        assert result is zone
        mock_db_session.add.assert_not_called()

    def test_creates_new_default_zone(self, service, mock_db, mock_db_session, mock_zone_repo):
        mock_zone_repo.get_by_name.return_value = None

        result = service.ensure_default_zone()

        assert result is not None
        assert result.name == "default"
        mock_db_session.add.assert_called_once()


class TestGetCoreChannels:
    def test_returns_all_channels(self, service, mock_db, mock_db_session, mock_channel_repo):
        channels = [make_channel(), make_channel()]
        mock_channel_repo.fetch_all.return_value = channels

        with patch("kronicle.services.core_service.OutputCoreChannel.from_db") as mock_from:
            mock_from.side_effect = lambda c: MagicMock(id=c.id)
            result = service.get_core_channels()

        assert len(result) == 2
        mock_channel_repo.fetch_all.assert_called_once_with(mock_db_session)

    def test_filters_by_zone(self, service, mock_db, mock_db_session, mock_zone_repo, mock_channel_repo):
        zone_id = uuid4()
        mock_zone_repo.get_by_id.return_value = make_zone(id=zone_id)
        channels = [make_channel(zone_id=zone_id)]
        mock_channel_repo.get_by_zone.return_value = channels

        with patch("kronicle.services.core_service.OutputCoreChannel.from_db") as mock_from:
            mock_from.side_effect = lambda c: MagicMock(id=c.id)
            result = service.get_core_channels(zone_id=zone_id)

        assert len(result) == 1
        mock_channel_repo.get_by_zone.assert_called_once_with(mock_db_session, zone_id=zone_id)

    def test_raises_if_zone_not_found(self, service, mock_db, mock_db_session, mock_zone_repo):
        mock_zone_repo.get_by_id.return_value = None
        with pytest.raises(NotFoundError, match="not found"):
            service.get_core_channels(zone_id=uuid4())


class TestGetCoreChannel:
    def test_returns_channel(self, service, mock_db, mock_db_session, mock_channel_repo):
        channel = make_channel()
        mock_channel_repo.get_by_id.return_value = channel

        with patch("kronicle.services.core_service.OutputCoreChannel.from_db") as mock_from:
            mock_from.return_value = MagicMock(id=channel.id)
            result = service.get_core_channel(channel.id)

        assert result is not None

    def test_returns_none_if_not_found(self, service, mock_db, mock_db_session, mock_channel_repo):
        mock_channel_repo.get_by_id.return_value = None
        result = service.get_core_channel(uuid4())
        assert result is None


class TestCreateCoreChannel:
    def test_creates_channel(self, service, mock_db, mock_db_session):
        channel_id = uuid4()
        zone_id = uuid4()
        channel = InputCoreChannel(id=channel_id, name="custom-name", zone_id=zone_id, details={"key": "val"})

        with patch("kronicle.services.core_service.OutputCoreChannel.from_db") as mock_from:
            mock_from.return_value = MagicMock(id=channel_id)
            result = service.create_core_channel(channel)

        assert result is not None
        mock_db_session.add.assert_called_once()
        added = mock_db_session.add.call_args[0][0]
        assert added.id == channel_id
        assert added.zone_id == zone_id
        assert added.name == "custom-name"
        assert added.details == {"key": "val"}


class TestEnsureChannelInZone:
    def test_creates_channel_if_not_exists(self, service, mock_db, mock_db_session, mock_zone_repo, mock_channel_repo):
        zone = make_zone()
        mock_zone_repo.get_by_id.return_value = zone

        channel_id = uuid4()
        mock_channel_repo.get_by_id.return_value = None

        with patch.object(service, "create_core_channel") as mock_create:
            mock_create.return_value = MagicMock()
            service.ensure_channel_in_zone(channel_id, zone.id)

        mock_create.assert_called_once_with(InputCoreChannel(id=channel_id, zone_id=zone.id))

    def test_raises_if_channel_in_different_zone(
        self, service, mock_db, mock_db_session, mock_zone_repo, mock_channel_repo
    ):
        zone_id = uuid4()
        other_zone_id = uuid4()
        mock_zone_repo.get_by_id.return_value = make_zone(id=zone_id)
        mock_channel_repo.get_by_id.return_value = make_channel(zone_id=other_zone_id)

        with pytest.raises(ConflictError, match="belongs to zone"):
            service.ensure_channel_in_zone(uuid4(), zone_id)

    def test_noop_if_channel_already_in_zone(
        self, service, mock_db, mock_db_session, mock_zone_repo, mock_channel_repo
    ):
        zone_id = uuid4()
        mock_zone_repo.get_by_id.return_value = make_zone(id=zone_id)
        mock_channel_repo.get_by_id.return_value = make_channel(zone_id=zone_id)

        service.ensure_channel_in_zone(uuid4(), zone_id)
        mock_db_session.add.assert_not_called()

    def test_raises_if_zone_not_found(self, service, mock_db, mock_db_session, mock_zone_repo):
        mock_zone_repo.get_by_id.return_value = None
        with pytest.raises(NotFoundError, match="not found"):
            service.ensure_channel_in_zone(uuid4(), uuid4())


class TestPatchCoreChannel:
    def test_updates_name(self, service, mock_db, mock_db_session, mock_channel_repo):
        channel = make_channel(name="old-name")
        mock_channel_repo.get_by_id.return_value = channel

        with patch("kronicle.services.core_service.OutputCoreChannel.from_db") as mock_from:
            mock_from.return_value = MagicMock()
            result = service.patch_core_channel(channel.id, name="new-name")

        assert result is not None
        assert channel.name == "new-name"
        mock_db_session.flush.assert_called_once()

    def test_updates_details(self, service, mock_db, mock_db_session, mock_channel_repo):
        channel = make_channel()
        mock_channel_repo.get_by_id.return_value = channel

        with patch("kronicle.services.core_service.OutputCoreChannel.from_db") as mock_from:
            mock_from.return_value = MagicMock()
            result = service.patch_core_channel(channel.id, details={"key": "val"})

        assert result is not None
        assert channel.details == {"key": "val"}

    def test_updates_zone(self, service, mock_db, mock_db_session, mock_channel_repo, mock_zone_repo):
        new_zone_id = uuid4()
        channel = make_channel()
        mock_channel_repo.get_by_id.return_value = channel
        mock_zone_repo.get_by_id.return_value = make_zone(id=new_zone_id)

        with patch("kronicle.services.core_service.OutputCoreChannel.from_db") as mock_from:
            mock_from.return_value = MagicMock()
            result = service.patch_core_channel(channel.id, zone_id=new_zone_id)

        assert result is not None
        assert channel.zone_id == new_zone_id

    def test_raises_if_channel_not_found(self, service, mock_db, mock_db_session, mock_channel_repo):
        mock_channel_repo.get_by_id.return_value = None
        with pytest.raises(NotFoundError, match="not found"):
            service.patch_core_channel(uuid4(), name="new-name")

    def test_raises_if_new_zone_not_found(self, service, mock_db, mock_db_session, mock_channel_repo, mock_zone_repo):
        channel = make_channel()
        mock_channel_repo.get_by_id.return_value = channel
        mock_zone_repo.get_by_id.return_value = None

        with pytest.raises(NotFoundError, match="not found"):
            service.patch_core_channel(channel.id, zone_id=uuid4())
