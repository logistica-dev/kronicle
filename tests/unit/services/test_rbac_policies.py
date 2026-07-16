# tests/unit/services/test_rbac_policies.py
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from kronicle.errors.error_types import NotFoundError
from kronicle.schemas.core.input_ressource_schema import InputCoreChannel, InputRow, InputZonePatch
from kronicle.schemas.payload.input_payload import InputPayload
from kronicle.schemas.rbac.input_policy_schemas import (
    InputChannelAccessProfile,
    InputRowAccessProfile,
    InputZoneAccessProfile,
)
from kronicle.schemas.rbac.input_role_schemas import InputRole
from kronicle.schemas.rbac.input_subject_schemas import InputSubject
from kronicle.schemas.rbac.safe_policy_schemas import (
    OutputChannelPolicy,
    OutputRowPolicy,
    OutputZonePolicy,
)
from tests.unit.services.conftest import (
    fake_channel_policy_mock,
    fake_group,
    fake_role,
    fake_user,
    fake_zone_policy_mock,
)


class TestZonePolicy:
    def test_create(self, rbac_service):
        sid, rid, zid = uuid4(), uuid4(), uuid4()
        role = fake_role(id=rid, name="role")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)

        with patch.object(OutputZonePolicy, "from_db") as mock_from_db:
            expected = MagicMock(spec=OutputZonePolicy)
            expected.role = MagicMock()
            expected.role.name = "role"
            expected.role.id = rid
            expected.subject = MagicMock()
            expected.subject.id = sid
            expected.zone = MagicMock()
            expected.zone.id = zid
            mock_from_db.return_value = expected

            result = rbac_service.create_zone_policy(
                subject=InputSubject(id=sid, type="user", user_id=sid),
                access_profile=InputZoneAccessProfile(
                    role=InputRole(id=rid),
                    zone=InputZonePatch(id=zid),
                ),
            )

            assert result.role.name == "role"
            assert result.subject.id == sid
            assert result.role.id == rid
            assert result.zone.id == zid

    def test_create_role_not_found(self, rbac_service):
        rbac_service._role_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Role"):
            rbac_service.create_zone_policy(
                subject=InputSubject(id=uuid4(), type="user", user_id=uuid4()),
                access_profile=InputZoneAccessProfile(
                    role=InputRole(id=uuid4()),
                    zone=InputZonePatch(id=uuid4()),
                ),
            )

    def test_list(self, rbac_service):
        zid = uuid4()
        policy = fake_zone_policy_mock(zone_id=zid)

        rbac_service._zone_policy_repo.get_policies_for_zone = MagicMock(return_value=[policy])

        result = rbac_service.list_policies_for_zone(zid)
        assert len(result) == 1
        assert isinstance(result[0], OutputZonePolicy)
        assert result[0].access_profile.zone.id == zid

    def test_delete(self, rbac_service):
        pid = uuid4()
        policy = fake_zone_policy_mock(id=pid)
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._zone_policy_repo.get_by_id = MagicMock(return_value=policy)

        result = rbac_service.delete_zone_policy(pid)
        db.delete.assert_called_once_with(policy)
        assert isinstance(result, OutputZonePolicy)

    def test_delete_not_found(self, rbac_service):
        rbac_service._zone_policy_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.delete_zone_policy(uuid4())


class TestChannelPolicy:
    def test_create(self, rbac_service):
        sid, rid, cid = uuid4(), uuid4(), uuid4()
        role = fake_role(id=rid, name="role")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)

        with patch.object(OutputChannelPolicy, "from_db") as mock_from_db:
            expected = MagicMock()
            expected.role = MagicMock()
            expected.role.name = "role"
            expected.role.id = rid
            expected.subject = MagicMock()
            expected.subject.id = sid
            expected.access_profile = MagicMock()
            expected.access_profile.role = MagicMock()
            expected.access_profile.role.id = rid
            expected.access_profile.channel = MagicMock()
            expected.access_profile.channel.id = cid
            mock_from_db.return_value = expected

            result = rbac_service.create_channel_policy(
                subject=InputSubject(id=sid, type="user", user_id=sid),
                access_profile=InputChannelAccessProfile(
                    role=InputRole(id=rid),
                    channel=InputPayload(id=cid),
                ),
            )

            assert result.role.name == "role"
            assert result.subject.id == sid
            assert result.access_profile.role.id == rid
            assert result.access_profile.channel.id == cid

    def test_create_role_not_found(self, rbac_service):
        rbac_service._role_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Role"):
            rbac_service.create_channel_policy(
                subject=InputSubject(id=uuid4(), type="user", user_id=uuid4()),
                access_profile=InputChannelAccessProfile(
                    role=InputRole(id=uuid4()),
                    channel=InputPayload(id=uuid4()),
                ),
            )

    def test_list(self, rbac_service):
        cid = uuid4()
        policy = fake_channel_policy_mock(channel_id=cid)

        rbac_service._channel_policy_repo.get_policies_for_channel = MagicMock(return_value=[policy])

        result = rbac_service.list_policies_for_channel(cid)
        assert len(result) == 1
        assert isinstance(result[0], OutputChannelPolicy)
        assert result[0].access_profile.channel.id == cid

    def test_delete(self, rbac_service):
        pid = uuid4()
        policy = fake_channel_policy_mock(id=pid)
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._channel_policy_repo.get_by_id = MagicMock(return_value=policy)

        result = rbac_service.delete_channel_policy(pid)
        db.delete.assert_called_once_with(policy)
        assert isinstance(result, OutputChannelPolicy)

    def test_delete_not_found(self, rbac_service):
        rbac_service._channel_policy_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.delete_channel_policy(uuid4())


# ==================================================================================================
# Relationship checks
# ==================================================================================================


class TestRowPolicy:
    def test_create(self, rbac_service):
        sid, rid, row_id = uuid4(), uuid4(), uuid4()
        role = fake_role(id=rid, name="role")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)

        with patch.object(OutputRowPolicy, "from_db") as mock_from_db:
            expected = MagicMock()
            expected.role = MagicMock()
            expected.role.id = rid
            expected.subject = MagicMock()
            expected.subject.id = sid
            expected.access_profile = MagicMock()
            expected.access_profile.role = MagicMock()
            expected.access_profile.role.id = rid
            expected.access_profile.row = MagicMock()
            expected.access_profile.row.id = row_id
            mock_from_db.return_value = expected

            result = rbac_service.create_row_policy(
                subject=InputSubject(id=sid, type="user", user_id=sid),
                access_profile=InputRowAccessProfile(
                    role=InputRole(id=rid),
                    row=InputRow(id=row_id, channel=InputCoreChannel(id=uuid4())),
                ),
            )

            assert result.subject.id == sid
            assert result.access_profile.role.id == rid
            assert result.access_profile.row.id == row_id

    def test_list(self, rbac_service):
        row_id = uuid4()
        policy = MagicMock()
        policy.id = uuid4()
        policy.name = "rp"
        policy.subject = MagicMock()
        policy.access_profile = MagicMock()
        policy.access_profile.row = MagicMock()
        policy.access_profile.row.id = row_id
        policy.access_profile.role = fake_role()
        policy.is_delegation = False
        rbac_service._row_policy_repo.get_policies_for_row = MagicMock(return_value=[policy])

        with patch.object(OutputRowPolicy, "from_db", return_value=MagicMock(spec=OutputRowPolicy)):
            result = rbac_service.list_policies_for_row(row_id)
        assert len(result) == 1

    def test_delete(self, rbac_service):
        pid = uuid4()
        policy = MagicMock()
        policy.id = pid
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._row_policy_repo.get_by_id = MagicMock(return_value=policy)

        with patch.object(OutputRowPolicy, "from_db", return_value=MagicMock(spec=OutputRowPolicy)):
            rbac_service.delete_row_policy(pid)
        db.delete.assert_called_once_with(policy)

    def test_delete_not_found(self, rbac_service):
        rbac_service._row_policy_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.delete_row_policy(uuid4())

    def test_patch(self, rbac_service):
        pid = uuid4()
        policy = MagicMock()
        policy.name = "old"
        policy.details = None
        rbac_service._row_policy_repo.get_by_id = MagicMock(return_value=policy)

        with patch.object(OutputRowPolicy, "from_db", return_value=MagicMock(spec=OutputRowPolicy)):
            rbac_service.patch_row_policy(pid, name="new", details={"k": "v"})
        assert policy.name == "new"
        assert policy.details == {"k": "v"}

    def test_patch_not_found(self, rbac_service):
        rbac_service._row_policy_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.patch_row_policy(uuid4(), name="x")


# ==================================================================================================
# add_row_read_policies
# ==================================================================================================


class TestCreatePolicyInternal:
    def test_existing_policy_returns_early(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        subj = MagicMock()
        subj.id = uuid4()
        subj.name = "subj"
        db_access = MagicMock()
        db_access.id = uuid4()
        db_access.name = "profile"

        policy_repo = MagicMock()
        existing_policy = MagicMock()
        policy_repo.get_by_subject_and_access_profile = MagicMock(return_value=existing_policy)

        output_cls = MagicMock()
        output_cls.from_db.return_value = MagicMock()

        result = rbac_service._create_policy(
            db,
            subj=subj,
            db_access=db_access,
            policy_repo=policy_repo,
            policy_cls=MagicMock(),
            output_cls=output_cls,
        )
        output_cls.from_db.assert_called_once_with(existing_policy)
        assert result is output_cls.from_db.return_value

    def test_default_name_generation(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        subj = MagicMock()
        subj.id = uuid4()
        subj.name = "test-subject"
        db_access = MagicMock()
        db_access.id = uuid4()
        db_access.name = "Long Access Profile Name Here"
        db_access.role_id = uuid4()

        policy_repo = MagicMock()
        policy_repo.get_by_subject_and_access_profile = MagicMock(return_value=None)

        PolicyCls = MagicMock()
        OutputCls = MagicMock()
        policy_instance = MagicMock()
        policy_instance.name = None
        policy_instance.details = None
        PolicyCls.return_value = policy_instance
        OutputCls.from_db.return_value = MagicMock()

        result = rbac_service._create_policy(
            db,
            subj=subj,
            db_access=db_access,
            policy_repo=policy_repo,
            policy_cls=PolicyCls,
            output_cls=OutputCls,
        )
        PolicyCls.assert_called_once()
        call_kwargs = PolicyCls.call_args[1]
        assert "Long Access Profile Name" in call_kwargs["name"]
        assert "test-subject" in call_kwargs["name"]
        assert result is OutputCls.from_db.return_value


# ==================================================================================================
# patch zone/channel/row policies
# ==================================================================================================


class TestPatchZonePolicy:
    def test_patch_name_and_details(self, rbac_service):
        pid = uuid4()
        policy = MagicMock()
        policy.name = "old"
        policy.details = None
        rbac_service._zone_policy_repo.get_by_id = MagicMock(return_value=policy)

        with patch.object(OutputZonePolicy, "from_db", return_value=MagicMock(spec=OutputZonePolicy)):
            rbac_service.patch_zone_policy(pid, name="new", details={"k": "v"})
        assert policy.name == "new"
        assert policy.details == {"k": "v"}

    def test_patch_not_found(self, rbac_service):
        rbac_service._zone_policy_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.patch_zone_policy(uuid4(), name="x")


class TestPatchChannelPolicy:
    def test_patch_name_and_details(self, rbac_service):
        pid = uuid4()
        policy = MagicMock()
        policy.name = "old"
        policy.details = None
        rbac_service._channel_policy_repo.get_by_id = MagicMock(return_value=policy)

        with patch.object(OutputChannelPolicy, "from_db", return_value=MagicMock(spec=OutputChannelPolicy)):
            rbac_service.patch_channel_policy(pid, name="new", details={"k": "v"})
        assert policy.name == "new"
        assert policy.details == {"k": "v"}

    def test_patch_not_found(self, rbac_service):
        rbac_service._channel_policy_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.patch_channel_policy(uuid4(), name="x")


class TestAddRowReadPolicies:
    def test_no_users_no_groups_returns_early(self, rbac_service):
        rbac_service._row_repo.save = MagicMock()
        rbac_service.add_row_read_policies(uuid4(), [1, 2], read_users=None, read_groups=None)
        rbac_service._row_repo.save.assert_not_called()

    def test_creates_policies_for_users(self, rbac_service):
        channel_id = uuid4()
        ts_row_ids = [1]
        user_name = "alice"
        role = fake_role(name="Reader")
        rbac_service._role_repo.get_by_name = MagicMock(return_value=role)
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        rbac_service._row_repo.save = MagicMock(return_value=MagicMock(id=uuid4()))
        rbac_service._row_access_profile_repo.create = MagicMock(
            return_value=MagicMock(id=uuid4(), name="row-ap", role=role)
        )
        subj = MagicMock()
        subj.id = uuid4()
        subj.name = user_name
        rbac_service._subject_repo.ensure_from_user = MagicMock(return_value=subj)
        rbac_service._subject_repo.ensure_from_group = MagicMock(return_value=subj)
        rbac_service._user_repo.get_by_name = MagicMock(return_value=fake_user(name=user_name))
        policy_repo = MagicMock()
        policy_repo.get_by_subject_and_access_profile = MagicMock(return_value=None)
        rbac_service._row_policy_repo = policy_repo
        PolicyCls = MagicMock()
        PolicyCls.return_value = MagicMock(name=None, details=None)
        rbac_service._create_policy = MagicMock()

        rbac_service.add_row_read_policies(channel_id, ts_row_ids, read_users=[user_name], read_groups=None)
        rbac_service._create_policy.assert_called()

    def test_creates_policies_for_groups(self, rbac_service):
        channel_id = uuid4()
        ts_row_ids = [1]
        group_name = "team-a"
        role = fake_role(name="Reader")
        rbac_service._role_repo.get_by_name = MagicMock(return_value=role)
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        rbac_service._row_repo.save = MagicMock(return_value=MagicMock(id=uuid4()))
        rbac_service._row_access_profile_repo.create = MagicMock(
            return_value=MagicMock(id=uuid4(), name="row-ap", role=role)
        )
        subj = MagicMock()
        subj.id = uuid4()
        subj.name = group_name
        rbac_service._subject_repo.ensure_from_group = MagicMock(return_value=subj)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=fake_group(name=group_name))
        rbac_service._create_policy = MagicMock()

        rbac_service.add_row_read_policies(channel_id, ts_row_ids, read_users=None, read_groups=[group_name])
        rbac_service._create_policy.assert_called()

    def test_skips_missing_users(self, rbac_service):
        channel_id = uuid4()
        ts_row_ids = [1]
        role = fake_role(name="Reader")
        rbac_service._role_repo.get_by_name = MagicMock(return_value=role)
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        rbac_service._row_repo.save = MagicMock(return_value=MagicMock(id=uuid4()))
        rbac_service._row_access_profile_repo.create = MagicMock(
            return_value=MagicMock(id=uuid4(), name="row-ap", role=role)
        )
        rbac_service._user_repo.get_by_name = MagicMock(return_value=None)
        rbac_service._subject_repo.ensure_from_user = MagicMock(side_effect=NotFoundError("User 'missing' not found"))
        rbac_service._create_policy = MagicMock()

        rbac_service.add_row_read_policies(channel_id, ts_row_ids, read_users=["missing"], read_groups=None)
        rbac_service._create_policy.assert_not_called()

    def test_skips_missing_groups(self, rbac_service):
        channel_id = uuid4()
        ts_row_ids = [1]
        role = fake_role(name="Reader")
        rbac_service._role_repo.get_by_name = MagicMock(return_value=role)
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        rbac_service._row_repo.save = MagicMock(return_value=MagicMock(id=uuid4()))
        rbac_service._row_access_profile_repo.create = MagicMock(
            return_value=MagicMock(id=uuid4(), name="row-ap", role=role)
        )
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        rbac_service._subject_repo.ensure_from_group = MagicMock(side_effect=NotFoundError("Group 'missing' not found"))
        rbac_service._create_policy = MagicMock()

        rbac_service.add_row_read_policies(channel_id, ts_row_ids, read_users=None, read_groups=["missing"])
        rbac_service._create_policy.assert_not_called()

    def test_multiple_rows(self, rbac_service):
        channel_id = uuid4()
        ts_row_ids = [1, 2]
        role = fake_role(name="Reader")
        rbac_service._role_repo.get_by_name = MagicMock(return_value=role)
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        rbac_service._row_repo.save = MagicMock(return_value=MagicMock(id=uuid4()))
        rbac_service._row_access_profile_repo.create = MagicMock(
            return_value=MagicMock(id=uuid4(), name="row-ap", role=role)
        )
        subj = MagicMock()
        subj.id = uuid4()
        subj.name = "alice"
        rbac_service._subject_repo.ensure_from_user = MagicMock(return_value=subj)
        rbac_service._user_repo.get_by_name = MagicMock(return_value=fake_user(name="alice"))
        rbac_service._create_policy = MagicMock()

        rbac_service.add_row_read_policies(channel_id, ts_row_ids, read_users=["alice"], read_groups=None)
        assert rbac_service._create_policy.call_count == 2


# ==================================================================================================
# list_policies (all types)
# ==================================================================================================


class TestListPoliciesAll:
    def test_list_zone_policies(self, rbac_service):
        rbac_service._zone_policy_repo.fetch_all = MagicMock(return_value=[])
        result = rbac_service.list_zone_policies()
        assert result == []

    def test_list_channel_policies(self, rbac_service):
        rbac_service._channel_policy_repo.fetch_all = MagicMock(return_value=[])
        result = rbac_service.list_channel_policies()
        assert result == []

    def test_list_row_policies(self, rbac_service):
        rbac_service._row_policy_repo.fetch_all = MagicMock(return_value=[])
        result = rbac_service.list_row_policies()
        assert result == []

    def test_list_policies(self, rbac_service):
        rbac_service._zone_policy_repo.fetch_all = MagicMock(return_value=[])
        rbac_service._channel_policy_repo.fetch_all = MagicMock(return_value=[])
        rbac_service._row_policy_repo.fetch_all = MagicMock(return_value=[])
        result = rbac_service.list_policies()
        assert "zone" in result
        assert "channel" in result
        assert "row" in result


# ==================================================================================================
# remove_user_from_group group not found
# ==================================================================================================


class TestPatchZonePolicyEdges:
    def test_patch_details_only(self, rbac_service):
        pid = uuid4()
        policy = MagicMock()
        policy.name = "keep"
        policy.details = {"old": True}
        rbac_service._zone_policy_repo.get_by_id = MagicMock(return_value=policy)

        with patch.object(OutputZonePolicy, "from_db", return_value=MagicMock(spec=OutputZonePolicy)):
            rbac_service.patch_zone_policy(pid, details={"new": True})
        assert policy.details == {"new": True}
        assert policy.name == "keep"


class TestPatchChannelPolicyEdges:
    def test_patch_details_only(self, rbac_service):
        pid = uuid4()
        policy = MagicMock()
        policy.name = "keep"
        policy.details = {"old": True}
        rbac_service._channel_policy_repo.get_by_id = MagicMock(return_value=policy)

        with patch.object(OutputChannelPolicy, "from_db", return_value=MagicMock(spec=OutputChannelPolicy)):
            rbac_service.patch_channel_policy(pid, details={"new": True})
        assert policy.details == {"new": True}
        assert policy.name == "keep"


# ==================================================================================================
# patch_row_policy extra edge
# ==================================================================================================


class TestPatchRowPolicyEdges:
    def test_patch_details_only(self, rbac_service):
        pid = uuid4()
        policy = MagicMock()
        policy.name = "keep"
        policy.details = {"old": True}
        rbac_service._row_policy_repo.get_by_id = MagicMock(return_value=policy)

        with patch.object(OutputRowPolicy, "from_db", return_value=MagicMock(spec=OutputRowPolicy)):
            rbac_service.patch_row_policy(pid, details={"new": True})
        assert policy.details == {"new": True}
        assert policy.name == "keep"


# ==================================================================================================
# Anonymous user permission leak: user_id=None must not match group subjects via IS NULL
# ==================================================================================================
