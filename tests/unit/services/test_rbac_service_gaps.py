# tests/unit/services/test_rbac_service_gaps.py
"""Focused unit tests for RBAC service branches not covered elsewhere (error paths, empty results, helpers)."""

from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from kronicle.db.core.models.core_row import CoreRow
from kronicle.db.rbac.links.rbac_policy import ChannelPolicy, RowPolicy, ZonePolicy
from kronicle.errors.error_types import NotFoundError
from kronicle.schemas.rbac.safe_introspect_schemas import OutputGroupPermissions, OutputUserPermissions
from kronicle.schemas.rbac.safe_policy_schemas import OutputChannelPolicy, OutputRowPolicy, OutputZonePolicy
from tests.unit.services.conftest import (
    _fake_core_channel,
    _fake_core_row,
    _fake_row_policy_mock,
    _fake_zone,
    fake_channel_policy_mock,
    fake_group,
    fake_group_role_link,
    fake_role,
    fake_user,
    fake_zone_policy_mock,
)


def _execute_result(values):
    result = MagicMock()
    result.scalars.return_value.all.return_value = values
    return result


def _spec_policy(cls, source):
    """Turn a fake policy into a MagicMock whose isinstance(cls) check succeeds, keeping valid attributes."""
    policy = MagicMock(spec=cls)
    policy.id = source.id
    policy.name = source.name
    policy.subject = source.subject
    policy.access_profile = source.access_profile
    policy.is_delegation = source.is_delegation
    return policy


# --------------------------------------------------------------------------------------------------
# Row creation helper
# --------------------------------------------------------------------------------------------------


class TestEnsureCoreRow:
    def test_returns_existing_row(self, rbac_service):
        row = MagicMock(id=uuid4())
        rbac_service._row_repo.get_by_channel_and_row_id = MagicMock(return_value=row)
        db = MagicMock()

        assert rbac_service._ensure_core_row(db, channel_id=uuid4(), timeseries_row_id=3) is row
        db.add.assert_not_called()

    def test_creates_row_when_missing(self, rbac_service):
        channel_id = uuid4()
        rbac_service._row_repo.get_by_channel_and_row_id = MagicMock(return_value=None)
        db = MagicMock()

        row = rbac_service._ensure_core_row(db, channel_id=channel_id, timeseries_row_id=7)

        assert isinstance(row, CoreRow)
        assert row.timeseries_row_id == 7
        assert row.channel_id == channel_id
        assert row.name == "row_7"
        db.add.assert_called_once_with(row)
        db.flush.assert_called_once()


# --------------------------------------------------------------------------------------------------
# Single-policy getters
# --------------------------------------------------------------------------------------------------


class TestPolicyGetters:
    def test_get_zone_policy(self, rbac_service):
        policy = fake_zone_policy_mock()
        rbac_service._zone_policy_repo.get_by_id = MagicMock(return_value=policy)

        result = rbac_service.get_zone_policy(policy.id)

        assert isinstance(result, OutputZonePolicy)
        assert result.id == policy.id

    def test_get_zone_policy_not_found(self, rbac_service):
        rbac_service._zone_policy_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.get_zone_policy(uuid4())

    def test_get_channel_policy(self, rbac_service):
        policy = fake_channel_policy_mock()
        rbac_service._channel_policy_repo.get_by_id = MagicMock(return_value=policy)

        result = rbac_service.get_channel_policy(policy.id)

        assert isinstance(result, OutputChannelPolicy)
        assert result.id == policy.id

    def test_get_channel_policy_not_found(self, rbac_service):
        rbac_service._channel_policy_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.get_channel_policy(uuid4())

    def test_get_row_policy(self, rbac_service):
        policy = _fake_row_policy_mock()
        rbac_service._row_policy_repo.get_by_id = MagicMock(return_value=policy)

        result = rbac_service.get_row_policy(policy.id)

        assert isinstance(result, OutputRowPolicy)
        assert result.id == policy.id

    def test_get_row_policy_not_found(self, rbac_service):
        rbac_service._row_policy_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.get_row_policy(uuid4())


# --------------------------------------------------------------------------------------------------
# Role / group relationship checks
# --------------------------------------------------------------------------------------------------


class TestRoleCheckEdges:
    def test_user_has_role_user_missing(self, rbac_service):
        rbac_service._user_repo.get_by_id = MagicMock(return_value=None)
        assert rbac_service.check_user_has_role(uuid4(), uuid4()) is None

    def test_user_has_role_role_missing(self, rbac_service):
        rbac_service._user_repo.get_by_id = MagicMock(return_value=fake_user())
        rbac_service._role_repo.get_by_id = MagicMock(return_value=None)
        assert rbac_service.check_user_has_role(uuid4(), uuid4()) is None

    def test_group_has_role_group_missing(self, rbac_service):
        rbac_service._group_repo.get_by_id = MagicMock(return_value=None)
        assert rbac_service.check_group_has_role(uuid4(), uuid4()) is None

    def test_group_has_role_role_missing(self, rbac_service):
        rbac_service._group_repo.get_by_id = MagicMock(return_value=fake_group())
        rbac_service._role_repo.get_by_id = MagicMock(return_value=None)
        assert rbac_service.check_group_has_role(uuid4(), uuid4()) is None

    def test_group_has_role_indirect_without_match(self, rbac_service):
        gid, rid, aid = uuid4(), uuid4(), uuid4()
        rbac_service._group_repo.get_by_id = MagicMock(return_value=fake_group(id=gid))
        rbac_service._role_repo.get_by_id = MagicMock(return_value=fake_role(id=rid))
        rbac_service._group_roles_repo.check_link = MagicMock(return_value=None)
        rbac_service._get_group_ancestor_ids = MagicMock(return_value={aid})
        rbac_service._group_roles_repo.get_role_link_for_groups = MagicMock(return_value=None)

        assert rbac_service.check_group_has_role(gid, rid, indirect=True) is None

    def test_user_in_group_user_missing(self, rbac_service):
        rbac_service._user_repo.get_by_id = MagicMock(return_value=None)
        assert rbac_service.check_user_in_group(uuid4(), uuid4()) is None

    def test_user_in_group_group_missing(self, rbac_service):
        rbac_service._user_repo.get_by_id = MagicMock(return_value=fake_user())
        rbac_service._group_repo.get_by_id = MagicMock(return_value=None)
        assert rbac_service.check_user_in_group(uuid4(), uuid4()) is None


# --------------------------------------------------------------------------------------------------
# Subject / hierarchy resolution helpers
# --------------------------------------------------------------------------------------------------


class TestSubjectResolution:
    def test_resolve_user_subject_ids_direct(self, rbac_service):
        db = MagicMock()
        db.execute.return_value = _execute_result([uuid4()])
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value=set())

        result = rbac_service._resolve_user_subject_ids(db, uuid4(), indirect=False)

        assert len(result) == 1

    def test_resolve_user_subject_ids_indirect_with_groups(self, rbac_service):
        db = MagicMock()
        db.execute.return_value = _execute_result([uuid4()])
        gid, aid = uuid4(), uuid4()
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value={gid})
        rbac_service._group_repo.get_by_id = MagicMock(return_value=fake_group(id=gid))
        rbac_service.group_hierarchy_service.ancestors = MagicMock(return_value=[fake_group(id=aid)])

        result = rbac_service._resolve_user_subject_ids(db, uuid4(), indirect=True)

        assert len(result) == 2

    def test_resolve_group_subject_ids_direct(self, rbac_service):
        db = MagicMock()
        db.execute.return_value = _execute_result([uuid4()])

        result = rbac_service._resolve_group_subject_ids(db, uuid4(), indirect=False)

        assert len(result) == 1

    def test_resolve_group_subject_ids_indirect(self, rbac_service):
        db = MagicMock()
        db.execute.return_value = _execute_result([uuid4()])
        gid = uuid4()
        rbac_service._group_repo.get_by_id = MagicMock(return_value=fake_group(id=gid))
        rbac_service.group_hierarchy_service.ancestors = MagicMock(return_value=[fake_group()])

        result = rbac_service._resolve_group_subject_ids(db, gid, indirect=True)

        assert len(result) == 1

    def test_resolve_group_subject_ids_indirect_group_missing(self, rbac_service):
        db = MagicMock()
        db.execute.return_value = _execute_result([uuid4()])
        rbac_service._group_repo.get_by_id = MagicMock(return_value=None)

        result = rbac_service._resolve_group_subject_ids(db, uuid4(), indirect=True)

        assert len(result) == 1


class TestZoneHierarchyHelpers:
    def test_get_zone_ancestor_ids_is_recursive(self, rbac_service):
        db = MagicMock()
        ancestor = uuid4()
        db.execute.side_effect = [_execute_result([ancestor]), _execute_result([])]

        assert rbac_service._get_zone_ancestor_ids(db, uuid4()) == {ancestor}

    def test_expand_zone_ids_with_ancestors(self, rbac_service):
        zone_id, ancestor = uuid4(), uuid4()
        rbac_service._get_zone_ancestor_ids = MagicMock(
            side_effect=lambda db, zid: {ancestor} if zid == zone_id else set()
        )

        assert rbac_service._expand_zone_ids_with_ancestors(MagicMock(), {zone_id}) == {zone_id, ancestor}

    def test_get_zone_descendant_ids_is_recursive(self, rbac_service):
        db = MagicMock()
        descendant = uuid4()
        db.execute.side_effect = [_execute_result([descendant]), _execute_result([])]

        assert rbac_service._get_zone_descendant_ids(db, uuid4()) == {descendant}


# --------------------------------------------------------------------------------------------------
# Permissions for missing user / group
# --------------------------------------------------------------------------------------------------


class TestMissingPermissions:
    def test_user_permissions_empty_when_user_missing(self, rbac_service):
        rbac_service._user_repo.get_by_id = MagicMock(return_value=None)
        result = rbac_service.get_user_permissions(uuid4())
        assert isinstance(result, OutputUserPermissions)
        assert result.user is None

    def test_group_permissions_empty_when_group_missing(self, rbac_service):
        rbac_service._group_repo.get_by_id = MagicMock(return_value=None)
        result = rbac_service.get_group_permissions(uuid4())
        assert isinstance(result, OutputGroupPermissions)
        assert result.group is None

    def test_group_permissions_with_inherited_ancestor_roles(self, rbac_service):
        gid = uuid4()
        rbac_service._group_repo.get_by_id = MagicMock(return_value=fake_group(id=gid))
        rbac_service._group_roles_repo.list_roles_for_group = MagicMock(return_value=[])
        rbac_service.group_hierarchy_service.ancestors_ids = MagicMock(return_value={uuid4()})
        rbac_service._group_roles_repo.list_roles_for_groups = MagicMock(return_value=[fake_group_role_link()])
        rbac_service._resolve_group_subject_ids = MagicMock(return_value=[])

        result = rbac_service.get_group_permissions(gid)

        assert len(result.indirect_roles) == 1


# --------------------------------------------------------------------------------------------------
# _build_resource_access concrete policy branches
# --------------------------------------------------------------------------------------------------


class TestBuildResourceAccess:
    def test_zone_policy(self, rbac_service):
        policy = _spec_policy(ZonePolicy, fake_zone_policy_mock())
        zone = _fake_zone()

        result = rbac_service._build_resource_access(policy, zone)

        assert isinstance(result.policy, OutputZonePolicy)
        assert result.resource.id == zone.id

    def test_channel_policy(self, rbac_service):
        policy = _spec_policy(ChannelPolicy, fake_channel_policy_mock())
        channel = _fake_core_channel()

        result = rbac_service._build_resource_access(policy, channel)

        assert isinstance(result.policy, OutputChannelPolicy)
        assert result.resource.id == channel.id

    def test_row_policy(self, rbac_service):
        policy = _spec_policy(RowPolicy, _fake_row_policy_mock())
        row = _fake_core_row()

        result = rbac_service._build_resource_access(policy, row)

        assert isinstance(result.policy, OutputRowPolicy)
        assert result.resource.id == row.id


# --------------------------------------------------------------------------------------------------
# User resource access edges
# --------------------------------------------------------------------------------------------------


class TestUserZoneEdges:
    def test_indirect_expands_ancestors(self, rbac_service):
        zone = _fake_zone()
        policy = fake_zone_policy_mock(zone_id=zone.id)
        rbac_service._resolve_user_subject_ids = MagicMock(return_value=[uuid4()])
        rbac_service._zone_policy_repo.get_policies_for_subjects = MagicMock(return_value=[policy])
        rbac_service._expand_zone_ids_with_ancestors = MagicMock(return_value={zone.id})
        rbac_service._zone_repo.get_by_id = MagicMock(return_value=zone)

        result = rbac_service.get_user_zones(uuid4(), indirect=True)

        assert len(result) == 1
        rbac_service._expand_zone_ids_with_ancestors.assert_called_once()

    def test_missing_zone_is_skipped(self, rbac_service):
        policy = fake_zone_policy_mock()
        rbac_service._resolve_user_subject_ids = MagicMock(return_value=[uuid4()])
        rbac_service._zone_policy_repo.get_policies_for_subjects = MagicMock(return_value=[policy])
        rbac_service._zone_repo.get_by_id = MagicMock(return_value=None)

        assert rbac_service.get_user_zones(uuid4()) == []


class TestUserChannelEdges:
    def test_indirect_expands_zones_and_descendants(self, rbac_service):
        zone = _fake_zone()
        channel = _fake_core_channel(zone_id=zone.id)
        zone_policy = fake_zone_policy_mock(zone_id=zone.id)
        rbac_service._resolve_user_subject_ids = MagicMock(return_value=[uuid4()])
        rbac_service._channel_policy_repo.get_policies_for_subjects = MagicMock(return_value=[])
        rbac_service._zone_policy_repo.get_policies_for_subjects = MagicMock(return_value=[zone_policy])
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value = _execute_result([])
        rbac_service._channel_repo.get_by_zone = MagicMock(return_value=[channel])
        rbac_service._channel_repo.get_by_id = MagicMock(return_value=channel)
        rbac_service._zone_repo.get_by_id = MagicMock(return_value=zone)

        result = rbac_service.get_user_channels(uuid4(), indirect=True)

        assert len(result) == 1
        assert result[0].resource.id == channel.id

    def test_missing_channel_is_skipped(self, rbac_service):
        policy = fake_channel_policy_mock()
        rbac_service._resolve_user_subject_ids = MagicMock(return_value=[uuid4()])
        rbac_service._channel_policy_repo.get_policies_for_subjects = MagicMock(return_value=[policy])
        rbac_service._zone_policy_repo.get_policies_for_subjects = MagicMock(return_value=[])
        rbac_service._channel_repo.get_by_id = MagicMock(return_value=None)

        assert rbac_service.get_user_channels(uuid4()) == []

    def test_no_subjects_returns_empty(self, rbac_service):
        rbac_service._resolve_user_subject_ids = MagicMock(return_value=[])
        assert rbac_service.get_user_channels(uuid4()) == []


class TestUserRowEdges:
    def test_no_subjects_returns_empty(self, rbac_service):
        rbac_service._resolve_user_subject_ids = MagicMock(return_value=[])
        assert rbac_service.get_user_rows(uuid4()) == []

    def test_missing_row_is_skipped(self, rbac_service):
        policy = _fake_row_policy_mock()
        rbac_service._resolve_user_subject_ids = MagicMock(return_value=[uuid4()])
        rbac_service._row_policy_repo.get_policies_for_subjects = MagicMock(return_value=[policy])
        rbac_service._row_repo.get_by_id = MagicMock(return_value=None)

        assert rbac_service.get_user_rows(uuid4()) == []


# --------------------------------------------------------------------------------------------------
# Group resource access edges
# --------------------------------------------------------------------------------------------------


class TestGroupZoneEdges:
    def test_no_subjects_returns_empty(self, rbac_service):
        rbac_service._resolve_group_subject_ids = MagicMock(return_value=[])
        assert rbac_service.get_group_zones(uuid4()) == []

    def test_indirect_expands_ancestors_and_skips_missing_zone(self, rbac_service):
        zone = _fake_zone()
        policy = fake_zone_policy_mock(zone_id=zone.id)
        rbac_service._resolve_group_subject_ids = MagicMock(return_value=[uuid4()])
        rbac_service._zone_policy_repo.get_policies_for_subjects = MagicMock(return_value=[policy])
        rbac_service._expand_zone_ids_with_ancestors = MagicMock(return_value={zone.id, uuid4()})
        rbac_service._zone_repo.get_by_id = MagicMock(side_effect=lambda db, id: zone if id == zone.id else None)

        result = rbac_service.get_group_zones(uuid4(), indirect=True)

        assert len(result) == 1
        assert result[0].resource.id == zone.id


class TestGroupChannelEdges:
    def test_no_subjects_returns_empty(self, rbac_service):
        rbac_service._resolve_group_subject_ids = MagicMock(return_value=[])
        assert rbac_service.get_group_channels(uuid4()) == []


class TestGroupRowEdges:
    def test_no_subjects_returns_empty(self, rbac_service):
        rbac_service._resolve_group_subject_ids = MagicMock(return_value=[])
        assert rbac_service.get_group_rows(uuid4()) == []

    def test_missing_row_is_skipped(self, rbac_service):
        policy = _fake_row_policy_mock()
        rbac_service._resolve_group_subject_ids = MagicMock(return_value=[uuid4()])
        rbac_service._row_policy_repo.get_policies_for_subjects = MagicMock(return_value=[policy])
        rbac_service._row_repo.get_by_id = MagicMock(return_value=None)

        assert rbac_service.get_group_rows(uuid4()) == []
