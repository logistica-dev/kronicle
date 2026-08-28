# tests/unit/services/test_rbac_profiles.py
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from kronicle.errors.error_types import BadRequestError, NotFoundError
from kronicle.schemas.core.input_ressource_schema import InputRow, InputZonePatch
from kronicle.schemas.payload.input_payload import InputPayload
from kronicle.schemas.rbac.input_policy_schemas import (
    InputChannelAccessProfile,
    InputRowAccessProfile,
    InputZoneAccessProfile,
)
from kronicle.schemas.rbac.input_role_schemas import InputRole
from kronicle.schemas.rbac.safe_policy_schemas import (
    OutputChannelAccessProfile,
    OutputRowAccessProfile,
    OutputZoneAccessProfile,
)
from tests.unit.services.conftest import _fake_core_channel, _fake_zone, fake_role


class TestZoneAccessProfileCRUD:
    def _profile_mock(self, **kwargs):
        profile = MagicMock()
        profile.id = kwargs.get("id", uuid4())
        profile.name = kwargs.get("name", "zone-profile")
        profile.description = kwargs.get("description", None)
        rid = kwargs.get("role_id", uuid4())
        profile.role_id = rid
        profile.role = fake_role(id=rid, name=kwargs.get("role_name", "r"))
        zid = kwargs.get("zone_id", uuid4())
        profile.zone_id = zid
        profile.zone = _fake_zone(id=zid, name=kwargs.get("zone_name", "z"))
        return profile

    def test_create(self, rbac_service):
        rid, zid = uuid4(), uuid4()
        profile = self._profile_mock(role_id=rid, zone_id=zid, description="desc")
        rbac_service._ensure_zone_access_profile = MagicMock(return_value=profile)

        out = rbac_service.create_zone_access_profile(
            profile_in=InputZoneAccessProfile(role=InputRole(id=rid), zone=InputZonePatch(id=zid), description="desc")
        )
        assert isinstance(out, OutputZoneAccessProfile)

    def test_list(self, rbac_service):
        profile = self._profile_mock()
        rbac_service._zone_access_profile_repo.fetch_all = MagicMock(return_value=[profile])

        result = rbac_service.list_zone_access_profiles()
        assert len(result) == 1
        assert isinstance(result[0], OutputZoneAccessProfile)

    def test_get(self, rbac_service):
        pid = uuid4()
        profile = self._profile_mock(id=pid)
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=profile)

        result = rbac_service.get_zone_access_profile(pid)
        assert isinstance(result, OutputZoneAccessProfile)

    def test_get_none(self, rbac_service):
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=None)
        assert rbac_service.get_zone_access_profile(uuid4()) is None

    def test_delete(self, rbac_service):
        pid = uuid4()
        profile = self._profile_mock(id=pid)
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=profile)

        result = rbac_service.delete_zone_access_profile(pid)
        db.delete.assert_called_once_with(profile)
        assert isinstance(result, OutputZoneAccessProfile)

    def test_delete_not_found(self, rbac_service):
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.delete_zone_access_profile(uuid4())


class TestChannelAccessProfileCRUD:
    def _profile_mock(self, **kwargs):
        profile = MagicMock()
        profile.id = kwargs.get("id", uuid4())
        profile.name = kwargs.get("name", "channel-profile")
        profile.description = kwargs.get("description", None)
        rid = kwargs.get("role_id", uuid4())
        profile.role_id = rid
        profile.role = fake_role(id=rid, name=kwargs.get("role_name", "r"))
        cid = kwargs.get("channel_id", uuid4())
        profile.channel_id = cid
        profile.channel = _fake_core_channel(id=cid, name=kwargs.get("channel_name", "c"))
        return profile

    def test_create(self, rbac_service):
        rid, cid = uuid4(), uuid4()
        profile = self._profile_mock(role_id=rid, channel_id=cid, description="desc")
        rbac_service._ensure_channel_access_profile = MagicMock(return_value=profile)

        out = rbac_service.create_channel_access_profile(
            profile_in=InputChannelAccessProfile(
                role=InputRole(id=rid), channel=InputPayload(id=cid), description="desc"
            )
        )
        assert isinstance(out, OutputChannelAccessProfile)

    def test_list(self, rbac_service):
        profile = self._profile_mock()
        rbac_service._channel_access_profile_repo.fetch_all = MagicMock(return_value=[profile])

        result = rbac_service.list_channel_access_profiles()
        assert len(result) == 1
        assert isinstance(result[0], OutputChannelAccessProfile)

    def test_get(self, rbac_service):
        pid = uuid4()
        profile = self._profile_mock(id=pid)
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=profile)

        result = rbac_service.get_channel_access_profile(pid)
        assert isinstance(result, OutputChannelAccessProfile)

    def test_get_none(self, rbac_service):
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=None)
        assert rbac_service.get_channel_access_profile(uuid4()) is None

    def test_delete(self, rbac_service):
        pid = uuid4()
        profile = self._profile_mock(id=pid)
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=profile)

        result = rbac_service.delete_channel_access_profile(pid)
        db.delete.assert_called_once_with(profile)
        assert isinstance(result, OutputChannelAccessProfile)

    def test_delete_not_found(self, rbac_service):
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.delete_channel_access_profile(uuid4())


# ==================================================================================================
# Policies
# ==================================================================================================


class TestEnsureZoneAccessProfile:
    def test_existing_by_id(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        pid = uuid4()
        existing = MagicMock()
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=existing)

        result = rbac_service._ensure_zone_access_profile(
            db, InputZoneAccessProfile(id=pid, role=InputRole(id=uuid4()), zone=InputZonePatch(id=uuid4()))
        )
        assert result is existing

    def test_existing_by_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        existing = MagicMock()
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._zone_access_profile_repo.get_by_name = MagicMock(return_value=existing)

        result = rbac_service._ensure_zone_access_profile(
            db, InputZoneAccessProfile(name="my-profile", role=InputRole(id=uuid4()), zone=InputZonePatch(id=uuid4()))
        )
        assert result is existing

    def test_create_new_generates_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rid, zid = uuid4(), uuid4()
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._zone_access_profile_repo.get_by_name = MagicMock(return_value=None)
        rbac_service._zone_access_profile_repo.get_by_role_and_zone = MagicMock(return_value=None)
        role = fake_role(id=rid, name="reader")
        zone = _fake_zone(id=zid, name="my_zone")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        rbac_service._zone_repo.get_by_id = MagicMock(return_value=zone)
        new_profile = MagicMock()
        new_profile.description = None
        new_profile.details = None
        rbac_service._zone_access_profile_repo.create = MagicMock(return_value=new_profile)

        result = rbac_service._ensure_zone_access_profile(
            db, InputZoneAccessProfile(role=InputRole(id=rid), zone=InputZonePatch(id=zid))
        )
        rbac_service._zone_access_profile_repo.create.assert_called_once()
        assert result is new_profile

    def test_create_new_with_description_and_details(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rid, zid = uuid4(), uuid4()
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._zone_access_profile_repo.get_by_name = MagicMock(return_value=None)
        rbac_service._zone_access_profile_repo.get_by_role_and_zone = MagicMock(return_value=None)
        role = fake_role(id=rid, name="r")
        zone = _fake_zone(id=zid, name="z")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        rbac_service._zone_repo.get_by_id = MagicMock(return_value=zone)
        new_profile = MagicMock()
        new_profile.description = None
        new_profile.details = None
        rbac_service._zone_access_profile_repo.create = MagicMock(return_value=new_profile)

        result = rbac_service._ensure_zone_access_profile(
            db,
            InputZoneAccessProfile(
                role=InputRole(id=rid),
                zone=InputZonePatch(id=zid),
                description="my desc",
                details={"k": "v"},
            ),
        )
        assert result.description == "my desc"
        assert result.details == {"k": "v"}

    def test_existing_by_role_and_zone(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rid, zid = uuid4(), uuid4()
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._zone_access_profile_repo.get_by_name = MagicMock(return_value=None)
        role = fake_role(id=rid, name="r")
        zone = _fake_zone(id=zid, name="z")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        rbac_service._zone_repo.get_by_id = MagicMock(return_value=zone)
        existing = MagicMock()
        rbac_service._zone_access_profile_repo.get_by_role_and_zone = MagicMock(return_value=existing)

        result = rbac_service._ensure_zone_access_profile(
            db, InputZoneAccessProfile(role=InputRole(id=rid), zone=InputZonePatch(id=zid))
        )
        assert result is existing


# ==================================================================================================
# _ensure_channel_access_profile internal logic
# ==================================================================================================


class TestEnsureChannelAccessProfile:
    def test_existing_by_id(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        pid = uuid4()
        existing = MagicMock()
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=existing)

        result = rbac_service._ensure_channel_access_profile(
            db, InputChannelAccessProfile(id=pid, role=InputRole(id=uuid4()), channel=InputPayload(id=uuid4()))
        )
        assert result is existing

    def test_existing_by_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        existing = MagicMock()
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._channel_access_profile_repo.get_by_name = MagicMock(return_value=existing)

        result = rbac_service._ensure_channel_access_profile(
            db,
            InputChannelAccessProfile(name="my-profile", role=InputRole(id=uuid4()), channel=InputPayload(id=uuid4())),
        )
        assert result is existing

    def test_create_new_strips_channel_prefix(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rid, cid = uuid4(), uuid4()
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._channel_access_profile_repo.get_by_name = MagicMock(return_value=None)
        rbac_service._channel_access_profile_repo.get_by_role_and_channel = MagicMock(return_value=None)
        role = fake_role(id=rid, name="reader")
        channel = _fake_core_channel(id=cid, name="channel_my_data")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        rbac_service._channel_repo.get_by_id = MagicMock(return_value=channel)
        new_profile = MagicMock()
        new_profile.description = None
        new_profile.details = None
        rbac_service._channel_access_profile_repo.create = MagicMock(return_value=new_profile)

        rbac_service._ensure_channel_access_profile(
            db, InputChannelAccessProfile(role=InputRole(id=rid), channel=InputPayload(id=cid))
        )
        call_kwargs = rbac_service._channel_access_profile_repo.create.call_args
        assert "my_data" in call_kwargs[1].get("name", call_kwargs[0][-1] if len(call_kwargs[0]) > 2 else "")

    def test_create_new_with_description_and_details(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rid, cid = uuid4(), uuid4()
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._channel_access_profile_repo.get_by_name = MagicMock(return_value=None)
        rbac_service._channel_access_profile_repo.get_by_role_and_channel = MagicMock(return_value=None)
        role = fake_role(id=rid, name="r")
        channel = _fake_core_channel(id=cid, name="chan")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        rbac_service._channel_repo.get_by_id = MagicMock(return_value=channel)
        new_profile = MagicMock()
        new_profile.description = None
        new_profile.details = None
        rbac_service._channel_access_profile_repo.create = MagicMock(return_value=new_profile)

        result = rbac_service._ensure_channel_access_profile(
            db,
            InputChannelAccessProfile(
                role=InputRole(id=rid),
                channel=InputPayload(id=cid),
                description="desc",
                details={"k": "v"},
            ),
        )
        assert result.description == "desc"
        assert result.details == {"k": "v"}

    def test_existing_by_role_and_channel(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rid, cid = uuid4(), uuid4()
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._channel_access_profile_repo.get_by_name = MagicMock(return_value=None)
        role = fake_role(id=rid, name="r")
        channel = _fake_core_channel(id=cid, name="c")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        rbac_service._channel_repo.get_by_id = MagicMock(return_value=channel)
        existing = MagicMock()
        rbac_service._channel_access_profile_repo.get_by_role_and_channel = MagicMock(return_value=existing)

        result = rbac_service._ensure_channel_access_profile(
            db, InputChannelAccessProfile(role=InputRole(id=rid), channel=InputPayload(id=cid))
        )
        assert result is existing


# ==================================================================================================
# _ensure_row_access_profile
# ==================================================================================================


class TestEnsureRowAccessProfile:
    def test_existing_by_id(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        pid = uuid4()
        existing = MagicMock()
        rbac_service._row_access_profile_repo.get_by_id = MagicMock(return_value=existing)

        row_id = 5
        channel = _fake_core_channel()
        result = rbac_service._ensure_row_access_profile(
            db,
            InputRowAccessProfile(id=pid, role=InputRole(id=uuid4()), row=InputRow(id=row_id, channel_id=channel.id)),
        )
        assert result is existing

    def test_existing_by_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        existing = MagicMock()
        rbac_service._row_access_profile_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._row_access_profile_repo.get_by_name = MagicMock(return_value=existing)

        row_id = 5
        channel = _fake_core_channel()
        result = rbac_service._ensure_row_access_profile(
            db,
            InputRowAccessProfile(
                name="my-row-profile",
                role=InputRole(id=uuid4()),
                row=InputRow(id=row_id, channel_id=channel.id),
            ),
        )
        assert result is existing

    def test_create_new(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rid = uuid4()
        rbac_service._row_access_profile_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._row_access_profile_repo.get_by_name = MagicMock(return_value=None)
        role = fake_role(id=rid, name="reader")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        new_profile = MagicMock()
        new_profile.description = None
        new_profile.details = None
        rbac_service._row_access_profile_repo.create = MagicMock(return_value=new_profile)

        row_id = 5
        channel = _fake_core_channel()
        result = rbac_service._ensure_row_access_profile(
            db,
            InputRowAccessProfile(
                role=InputRole(id=rid),
                row=InputRow(id=row_id, channel_id=channel.id),
                description="desc",
                details={"k": "v"},
            ),
        )
        assert result.description == "desc"
        assert result.details == {"k": "v"}

    def test_create_new_generates_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rid = uuid4()
        rbac_service._row_access_profile_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._row_access_profile_repo.get_by_name = MagicMock(return_value=None)
        role = fake_role(id=rid, name="reader")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        new_profile = MagicMock()
        new_profile.description = None
        new_profile.details = None
        rbac_service._row_access_profile_repo.create = MagicMock(return_value=new_profile)

        row_id = 5
        channel = _fake_core_channel()
        rbac_service._ensure_row_access_profile(
            db,
            InputRowAccessProfile(
                role=InputRole(id=rid),
                row=InputRow(id=row_id, channel_id=channel.id),
            ),
        )
        rbac_service._row_access_profile_repo.create.assert_called_once()


# ==================================================================================================
# _resolve_role / _resolve_zone / _resolve_channel
# ==================================================================================================


class TestRowAccessProfileCRUD:
    def _profile_mock(self, **kwargs):
        profile = MagicMock()
        profile.id = kwargs.get("id", uuid4())
        profile.name = kwargs.get("name", "row-profile")
        profile.description = kwargs.get("description", None)
        rid = kwargs.get("role_id", uuid4())
        profile.role_id = rid
        profile.role = fake_role(id=rid, name=kwargs.get("role_name", "r"))
        row_id = kwargs.get("row_id", uuid4())
        profile.row_id = row_id
        row = MagicMock()
        row.id = row_id
        row.name = "row"
        profile.row = row
        return profile

    def test_create(self, rbac_service):
        profile = self._profile_mock(description="desc")
        rbac_service._ensure_row_access_profile = MagicMock(return_value=profile)

        with patch.object(OutputRowAccessProfile, "from_db", return_value=MagicMock(spec=OutputRowAccessProfile)):
            out = rbac_service.create_row_access_profile(
                profile_in=InputRowAccessProfile(
                    role=InputRole(id=uuid4()),
                    row=InputRow(id=5, channel_id=uuid4()),
                    description="desc",
                )
            )
        assert isinstance(out, OutputRowAccessProfile)

    def test_list(self, rbac_service):
        profile = self._profile_mock()
        rbac_service._row_access_profile_repo.fetch_all = MagicMock(return_value=[profile])

        with patch.object(OutputRowAccessProfile, "from_db", return_value=MagicMock(spec=OutputRowAccessProfile)):
            result = rbac_service.list_row_access_profiles()
        assert len(result) == 1
        assert isinstance(result[0], OutputRowAccessProfile)

    def test_get(self, rbac_service):
        pid = uuid4()
        profile = self._profile_mock(id=pid)
        rbac_service._row_access_profile_repo.get_by_id = MagicMock(return_value=profile)

        with patch.object(OutputRowAccessProfile, "from_db", return_value=MagicMock(spec=OutputRowAccessProfile)):
            result = rbac_service.get_row_access_profile(pid)
        assert isinstance(result, OutputRowAccessProfile)

    def test_get_none(self, rbac_service):
        rbac_service._row_access_profile_repo.get_by_id = MagicMock(return_value=None)
        assert rbac_service.get_row_access_profile(uuid4()) is None

    def test_patch(self, rbac_service):
        pid = uuid4()
        profile = self._profile_mock(id=pid, name="old")
        rbac_service._row_access_profile_repo.get_by_id = MagicMock(return_value=profile)
        rid = uuid4()
        role = fake_role(id=rid, name="new-role")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)

        with patch.object(OutputRowAccessProfile, "from_db", return_value=MagicMock(spec=OutputRowAccessProfile)):
            rbac_service.patch_row_access_profile(
                pid, name="new", description="d", details={"k": "v"}, role=InputRole(id=rid)
            )
        assert profile.name == "new"
        assert profile.description == "d"
        assert profile.details == {"k": "v"}
        assert profile.role_id == rid

    def test_patch_not_found(self, rbac_service):
        rbac_service._row_access_profile_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.patch_row_access_profile(uuid4(), name="x")

    def test_delete(self, rbac_service):
        pid = uuid4()
        profile = self._profile_mock(id=pid)
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._row_access_profile_repo.get_by_id = MagicMock(return_value=profile)

        with patch.object(OutputRowAccessProfile, "from_db", return_value=MagicMock(spec=OutputRowAccessProfile)):
            rbac_service.delete_row_access_profile(pid)
        db.delete.assert_called_once_with(profile)

    def test_delete_not_found(self, rbac_service):
        rbac_service._row_access_profile_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.delete_row_access_profile(uuid4())


# ==================================================================================================
# list_access_profiles
# ==================================================================================================


class TestListAccessProfiles:
    def test_list_access_profiles(self, rbac_service):
        rbac_service._zone_access_profile_repo.fetch_all = MagicMock(return_value=[])
        rbac_service._channel_access_profile_repo.fetch_all = MagicMock(return_value=[])
        rbac_service._row_access_profile_repo.fetch_all = MagicMock(return_value=[])

        result = rbac_service.list_access_profiles()
        assert "zone" in result
        assert "channel" in result
        assert "row" in result
        assert len(result["zone"]) == 0
        assert len(result["channel"]) == 0
        assert len(result["row"]) == 0


# ==================================================================================================
# _create_policy existing + default name
# ==================================================================================================


class TestPatchZoneAccessProfile:
    def test_patch_name_and_role(self, rbac_service):
        pid = uuid4()
        profile = MagicMock()
        profile.name = "old"
        profile.description = None
        profile.details = None
        rid = uuid4()
        role = fake_role(id=rid, name="new-role")
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=profile)
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)

        with patch.object(OutputZoneAccessProfile, "from_db", return_value=MagicMock(spec=OutputZoneAccessProfile)):
            rbac_service.patch_zone_access_profile(pid, name="new-name", role=InputRole(id=rid))
        assert profile.name == "new-name"
        assert profile.role_id == rid

    def test_patch_description_and_details(self, rbac_service):
        pid = uuid4()
        profile = MagicMock()
        profile.name = "p"
        profile.description = "old"
        profile.details = {}
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=profile)

        with patch.object(OutputZoneAccessProfile, "from_db", return_value=MagicMock(spec=OutputZoneAccessProfile)):
            rbac_service.patch_zone_access_profile(pid, description="new desc", details={"k": "v"})
        assert profile.description == "new desc"
        assert profile.details == {"k": "v"}

    def test_patch_not_found(self, rbac_service):
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.patch_zone_access_profile(uuid4(), name="x")


class TestPatchChannelAccessProfile:
    def test_patch_name_and_role(self, rbac_service):
        pid = uuid4()
        profile = MagicMock()
        profile.name = "old"
        profile.description = None
        profile.details = None
        rid = uuid4()
        role = fake_role(id=rid, name="new-role")
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=profile)
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)

        with patch.object(
            OutputChannelAccessProfile, "from_db", return_value=MagicMock(spec=OutputChannelAccessProfile)
        ):
            rbac_service.patch_channel_access_profile(pid, name="new-name", role=InputRole(id=rid))
        assert profile.name == "new-name"
        assert profile.role_id == rid

    def test_patch_description_and_details(self, rbac_service):
        pid = uuid4()
        profile = MagicMock()
        profile.name = "p"
        profile.description = "old"
        profile.details = {}
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=profile)

        with patch.object(
            OutputChannelAccessProfile, "from_db", return_value=MagicMock(spec=OutputChannelAccessProfile)
        ):
            rbac_service.patch_channel_access_profile(pid, description="new desc", details={"k": "v"})
        assert profile.description == "new desc"
        assert profile.details == {"k": "v"}

    def test_patch_not_found(self, rbac_service):
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.patch_channel_access_profile(uuid4(), name="x")


# ==================================================================================================
# Row access profiles CRUD
# ==================================================================================================


class TestResolvers:
    def test_resolve_role_by_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        role = fake_role(name="reader")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._role_repo.get_by_name = MagicMock(return_value=role)

        result = rbac_service._resolve_role(db, InputRole(name="reader"))
        assert result.name == "reader"

    def test_resolve_role_by_name_not_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._role_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._role_repo.get_by_name = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Role"):
            rbac_service._resolve_role(db, InputRole(name="nonexistent"))

    def test_resolve_zone_by_id_not_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._zone_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Zone"):
            rbac_service._resolve_zone(db, InputZonePatch(id=uuid4()))

    def test_resolve_zone_by_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        zone = _fake_zone(name="prod")
        rbac_service._zone_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._zone_repo.get_by_name = MagicMock(return_value=zone)

        result = rbac_service._resolve_zone(db, InputZonePatch(name="prod"))
        assert result.name == "prod"

    def test_resolve_zone_by_name_not_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._zone_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._zone_repo.get_by_name = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Zone"):
            rbac_service._resolve_zone(db, InputZonePatch(name="nope"))

    def test_resolve_zone_missing_id_and_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        with pytest.raises(BadRequestError, match="Either id or name"):
            rbac_service._resolve_zone(db, InputZonePatch(id=None, name=None))

    def test_resolve_role_missing_id_and_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        with pytest.raises(BadRequestError, match="Either id or name"):
            rbac_service._resolve_role(db, InputRole())

    def test_resolve_channel_missing_id_and_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        with pytest.raises(BadRequestError, match="Either id or name"):
            rbac_service._resolve_channel(db, InputPayload())

    def test_resolve_channel_by_id_not_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._channel_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Channel"):
            rbac_service._resolve_channel(db, InputPayload(id=uuid4()))

    def test_resolve_channel_by_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        channel = _fake_core_channel(name="data-ch")
        rbac_service._channel_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._channel_repo.get_by_name = MagicMock(return_value=channel)

        result = rbac_service._resolve_channel(db, InputPayload(name="data-ch"))
        assert result.name == "data-ch"

    def test_resolve_channel_by_name_not_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._channel_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._channel_repo.get_by_name = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Channel"):
            rbac_service._resolve_channel(db, InputPayload(name="nope"))


# ==================================================================================================
# patch access profiles
# ==================================================================================================
