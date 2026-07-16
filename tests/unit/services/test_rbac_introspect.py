# tests/unit/services/test_rbac_introspect.py
"""Unit tests for RBAC introspection methods (permissions, resource access, resource policies)."""

from unittest.mock import MagicMock
from uuid import uuid4

from tests.unit.services.conftest import (
    _fake_row_policy_mock,
    _fake_zone,
    fake_channel_policy_mock,
    fake_group,
    fake_role,
    fake_zone_policy_mock,
)

# --------------------------------------------------------------------------------------------------
# Introspection helpers (lightweight, specific to introspection tests)
# --------------------------------------------------------------------------------------------------


def _fake_channel(id=None, name="ch", zone_id=None):
    ch = MagicMock()
    ch.id = id or uuid4()
    ch.name = name
    ch.zone_id = zone_id
    ch.details = {}
    return ch


def _fake_row(id=None, name="row", channel_id=None):
    r = MagicMock()
    r.id = id or uuid4()
    r.name = name
    r.channel_id = channel_id
    r.details = {}
    return r


# --------------------------------------------------------------------------------------------------
# Permissions introspection
# --------------------------------------------------------------------------------------------------


class TestGetUserPermissions:
    def test_returns_direct_roles_and_group_roles(self, rbac_service):
        uid = uuid4()
        rid1 = uuid4()
        rid2 = uuid4()
        gid = uuid4()
        subj_id = uuid4()

        rbac_service._user_roles_repo.get_role_ids_for_user = MagicMock(return_value=[rid1])
        rbac_service._role_repo.get_by_id = MagicMock(return_value=fake_role(id=rid1, name="admin"))
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value=[gid])
        rbac_service._group_repo.get_by_id = MagicMock(return_value=fake_group(id=gid, name="eng"))
        rbac_service._group_roles_repo.get_role_ids_for_group = MagicMock(return_value=[rid2])
        rbac_service._resolve_user_subject_ids = MagicMock(return_value=[subj_id])
        rbac_service._zone_policy_repo.get_policies_for_subjects = MagicMock(return_value=[])
        rbac_service._channel_policy_repo.get_policies_for_subjects = MagicMock(return_value=[])
        rbac_service._row_policy_repo.get_policies_for_subjects = MagicMock(return_value=[])

        result = rbac_service.get_user_permissions(uid)
        assert len(result["direct_roles"]) == 1
        assert len(result["group_roles"]) == 1
        assert result["group_roles"][0]["group"].name == "eng"
        assert result["group_roles"][0]["role"].name == "admin"

    def test_empty_when_no_subjects(self, rbac_service):
        uid = uuid4()

        rbac_service._user_roles_repo.get_role_ids_for_user = MagicMock(return_value=[])
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value=[])
        rbac_service._resolve_user_subject_ids = MagicMock(return_value=[])
        rbac_service._zone_policy_repo.get_policies_for_subjects = MagicMock(return_value=[])
        rbac_service._channel_policy_repo.get_policies_for_subjects = MagicMock(return_value=[])
        rbac_service._row_policy_repo.get_policies_for_subjects = MagicMock(return_value=[])

        result = rbac_service.get_user_permissions(uid)
        assert result["direct_roles"] == []
        assert result["group_roles"] == []
        assert result["zone_policies"] == []
        assert result["channel_policies"] == []
        assert result["row_policies"] == []


class TestGetGroupPermissions:
    def test_returns_direct_group_roles(self, rbac_service):
        gid = uuid4()
        rid = uuid4()
        subj_id = uuid4()

        rbac_service._group_roles_repo.get_role_ids_for_group = MagicMock(return_value=[rid])
        rbac_service._role_repo.get_by_id = MagicMock(return_value=fake_role(id=rid, name="editor"))
        rbac_service._resolve_group_subject_ids = MagicMock(return_value=[subj_id])
        rbac_service._zone_policy_repo.get_policies_for_subjects = MagicMock(return_value=[])
        rbac_service._channel_policy_repo.get_policies_for_subjects = MagicMock(return_value=[])
        rbac_service._row_policy_repo.get_policies_for_subjects = MagicMock(return_value=[])

        result = rbac_service.get_group_permissions(gid)
        assert len(result["direct_roles"]) == 1
        assert result["direct_roles"][0].name == "editor"
        assert result["group_roles"] == []


# --------------------------------------------------------------------------------------------------
# User resource access
# --------------------------------------------------------------------------------------------------


class TestGetUserZones:
    def test_returns_zone_access(self, rbac_service):
        uid = uuid4()
        zone = _fake_zone(name="myzone")
        subj_id = uuid4()
        policy = fake_zone_policy_mock(subject_id=subj_id, zone_id=zone.id)

        rbac_service._resolve_user_subject_ids = MagicMock(return_value=[subj_id])
        rbac_service._zone_policy_repo.get_policies_for_subjects = MagicMock(return_value=[policy])
        rbac_service._zone_repo.get_by_id = MagicMock(return_value=zone)

        result = rbac_service.get_user_zones(uid)
        assert len(result) == 1
        assert result[0].resource.name == "myzone"

    def test_empty_when_no_subjects(self, rbac_service):
        uid = uuid4()
        rbac_service._resolve_user_subject_ids = MagicMock(return_value=[])

        result = rbac_service.get_user_zones(uid)
        assert result == []


class TestGetUserChannels:
    def test_returns_direct_channel_policy(self, rbac_service):
        uid = uuid4()
        ch = _fake_channel(name="ch1")
        subj_id = uuid4()
        policy = fake_channel_policy_mock(subject_id=subj_id, channel_id=ch.id)

        rbac_service._resolve_user_subject_ids = MagicMock(return_value=[subj_id])
        rbac_service._channel_policy_repo.get_policies_for_subjects = MagicMock(return_value=[policy])
        rbac_service._zone_policy_repo.get_policies_for_subjects = MagicMock(return_value=[])
        rbac_service._channel_repo.get_by_id = MagicMock(return_value=ch)

        result = rbac_service.get_user_channels(uid)
        assert len(result) == 1
        assert result[0].resource.name == "ch1"

    def test_zone_policy_expands_to_channels(self, rbac_service):
        uid = uuid4()
        zone = _fake_zone(name="z1")
        ch = _fake_channel(name="ch-in-zone", zone_id=zone.id)
        subj_id = uuid4()
        zone_policy = fake_zone_policy_mock(subject_id=subj_id, zone_id=zone.id)

        rbac_service._resolve_user_subject_ids = MagicMock(return_value=[subj_id])
        rbac_service._channel_policy_repo.get_policies_for_subjects = MagicMock(return_value=[])
        rbac_service._zone_policy_repo.get_policies_for_subjects = MagicMock(return_value=[zone_policy])
        rbac_service._channel_repo.get_by_zone = MagicMock(return_value=[ch])
        rbac_service._channel_repo.get_by_id = MagicMock(return_value=ch)
        rbac_service._zone_repo.get_by_id = MagicMock(return_value=zone)

        result = rbac_service.get_user_channels(uid)
        assert len(result) == 1
        assert result[0].resource.name == "ch-in-zone"
        assert result[0].parent.name == "z1"


class TestGetUserRows:
    def test_returns_row_with_parent_channel(self, rbac_service):
        uid = uuid4()
        ch = _fake_channel(name="ch1")
        row = _fake_row(name="r1", channel_id=ch.id)
        subj_id = uuid4()
        policy = _fake_row_policy_mock(subject_id=subj_id, row_id=row.id)

        rbac_service._resolve_user_subject_ids = MagicMock(return_value=[subj_id])
        rbac_service._row_policy_repo.get_policies_for_subjects = MagicMock(return_value=[policy])
        rbac_service._row_repo.get_by_id = MagicMock(return_value=row)
        rbac_service._channel_repo.get_by_id = MagicMock(return_value=ch)

        result = rbac_service.get_user_rows(uid)
        assert len(result) == 1
        assert result[0].resource.name == "r1"
        assert result[0].parent.name == "ch1"


class TestGetUserResources:
    def test_aggregates_all_resource_types(self, rbac_service):
        uid = uuid4()
        zone = _fake_zone(name="z1")
        ch = _fake_channel(name="ch1")
        row = _fake_row(name="r1")
        subj_id = uuid4()

        zp = fake_zone_policy_mock(subject_id=subj_id, zone_id=zone.id)
        cp = fake_channel_policy_mock(subject_id=subj_id, channel_id=ch.id)
        rp = _fake_row_policy_mock(subject_id=subj_id, row_id=row.id)

        rbac_service._resolve_user_subject_ids = MagicMock(return_value=[subj_id])
        rbac_service._zone_policy_repo.get_policies_for_subjects = MagicMock(return_value=[zp])
        rbac_service._channel_policy_repo.get_policies_for_subjects = MagicMock(return_value=[cp])
        rbac_service._row_policy_repo.get_policies_for_subjects = MagicMock(return_value=[rp])
        rbac_service._zone_repo.get_by_id = MagicMock(return_value=zone)
        rbac_service._channel_repo.get_by_id = MagicMock(return_value=ch)
        rbac_service._channel_repo.get_by_zone = MagicMock(return_value=[])
        rbac_service._row_repo.get_by_id = MagicMock(return_value=row)

        result = rbac_service.get_user_resources(uid)
        assert len(result["zones"]) == 1
        assert len(result["channels"]) == 1
        assert len(result["rows"]) == 1


# --------------------------------------------------------------------------------------------------
# Group resource access
# --------------------------------------------------------------------------------------------------


class TestGetGroupZones:
    def test_returns_zone_access_for_group(self, rbac_service):
        gid = uuid4()
        zone = _fake_zone(name="gzone")
        subj_id = uuid4()
        policy = fake_zone_policy_mock(subject_id=subj_id, zone_id=zone.id)

        rbac_service._resolve_group_subject_ids = MagicMock(return_value=[subj_id])
        rbac_service._zone_policy_repo.get_policies_for_subjects = MagicMock(return_value=[policy])
        rbac_service._zone_repo.get_by_id = MagicMock(return_value=zone)

        result = rbac_service.get_group_zones(gid)
        assert len(result) == 1
        assert result[0].resource.name == "gzone"


class TestGetGroupChannels:
    def test_returns_channel_access_for_group(self, rbac_service):
        gid = uuid4()
        ch = _fake_channel(name="gch")
        subj_id = uuid4()
        policy = fake_channel_policy_mock(subject_id=subj_id, channel_id=ch.id)

        rbac_service._resolve_group_subject_ids = MagicMock(return_value=[subj_id])
        rbac_service._channel_policy_repo.get_policies_for_subjects = MagicMock(return_value=[policy])
        rbac_service._zone_policy_repo.get_policies_for_subjects = MagicMock(return_value=[])
        rbac_service._channel_repo.get_by_id = MagicMock(return_value=ch)

        result = rbac_service.get_group_channels(gid)
        assert len(result) == 1
        assert result[0].resource.name == "gch"


class TestGetGroupRows:
    def test_returns_row_access_for_group(self, rbac_service):
        gid = uuid4()
        ch = _fake_channel(name="gch")
        row = _fake_row(name="grow", channel_id=ch.id)
        subj_id = uuid4()
        policy = _fake_row_policy_mock(subject_id=subj_id, row_id=row.id)

        rbac_service._resolve_group_subject_ids = MagicMock(return_value=[subj_id])
        rbac_service._row_policy_repo.get_policies_for_subjects = MagicMock(return_value=[policy])
        rbac_service._row_repo.get_by_id = MagicMock(return_value=row)
        rbac_service._channel_repo.get_by_id = MagicMock(return_value=ch)

        result = rbac_service.get_group_rows(gid)
        assert len(result) == 1
        assert result[0].resource.name == "grow"
        assert result[0].parent.name == "gch"


class TestGetGroupResources:
    def test_aggregates_all_resource_types(self, rbac_service):
        gid = uuid4()
        zone = _fake_zone(name="gz")
        ch = _fake_channel(name="gc")
        row = _fake_row(name="gr")
        subj_id = uuid4()

        zp = fake_zone_policy_mock(subject_id=subj_id, zone_id=zone.id)
        cp = fake_channel_policy_mock(subject_id=subj_id, channel_id=ch.id)
        rp = _fake_row_policy_mock(subject_id=subj_id, row_id=row.id)

        rbac_service._resolve_group_subject_ids = MagicMock(return_value=[subj_id])
        rbac_service._zone_policy_repo.get_policies_for_subjects = MagicMock(return_value=[zp])
        rbac_service._channel_policy_repo.get_policies_for_subjects = MagicMock(return_value=[cp])
        rbac_service._row_policy_repo.get_policies_for_subjects = MagicMock(return_value=[rp])
        rbac_service._zone_repo.get_by_id = MagicMock(return_value=zone)
        rbac_service._channel_repo.get_by_id = MagicMock(return_value=ch)
        rbac_service._channel_repo.get_by_zone = MagicMock(return_value=[])
        rbac_service._row_repo.get_by_id = MagicMock(return_value=row)

        result = rbac_service.get_group_resources(gid)
        assert len(result["zones"]) == 1
        assert len(result["channels"]) == 1
        assert len(result["rows"]) == 1


# --------------------------------------------------------------------------------------------------
# Resource-level policy and access-profile lists
# --------------------------------------------------------------------------------------------------


class TestResourcePolicyEndpoints:
    def test_get_zone_policies(self, rbac_service):
        zid = uuid4()
        p1 = fake_zone_policy_mock(zone_id=zid)

        rbac_service._zone_policy_repo.get_policies_for_zone = MagicMock(return_value=[p1])

        result = rbac_service.get_zone_policies(zid)
        assert len(result) == 1

    def test_get_zone_access_profiles(self, rbac_service):
        zid = uuid4()
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [MagicMock()]
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value = mock_result

        result = rbac_service.get_zone_access_profiles(zid)
        assert len(result) == 1

    def test_get_channel_policies(self, rbac_service):
        cid = uuid4()
        p1 = fake_channel_policy_mock(channel_id=cid)

        rbac_service._channel_policy_repo.get_policies_for_channel = MagicMock(return_value=[p1])

        result = rbac_service.get_channel_policies(cid)
        assert len(result) == 1

    def test_get_channel_access_profiles(self, rbac_service):
        cid = uuid4()
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [MagicMock(), MagicMock()]
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value = mock_result

        result = rbac_service.get_channel_access_profiles(cid)
        assert len(result) == 2

    def test_get_row_policies(self, rbac_service):
        rid = uuid4()
        p1 = _fake_row_policy_mock(row_id=rid)

        rbac_service._row_policy_repo.get_policies_for_row = MagicMock(return_value=[p1])

        result = rbac_service.get_row_policies(rid)
        assert len(result) == 1

    def test_get_row_access_profiles(self, rbac_service):
        rid = uuid4()
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value = mock_result

        result = rbac_service.get_row_access_profiles(rid)
        assert result == []
