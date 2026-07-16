from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from kronicle.api.rbac_routes import (
    add_user_to_group,
    assign_role_to_group,
    assign_role_to_user,
    check_group_role,
    check_user_group,
    check_user_role,
    create_channel_access_profile,
    create_channel_policy,
    create_group,
    create_role,
    create_row_access_profile,
    create_row_policy,
    create_user,
    create_zone_access_profile,
    create_zone_policy,
    delete_channel_access_profile,
    delete_channel_policy,
    delete_group,
    delete_role,
    delete_row_access_profile,
    delete_row_policy,
    delete_user,
    delete_user_by_id,
    delete_zone_access_profile,
    delete_zone_policy,
    get_channel_access_profile,
    get_group,
    get_group_channels,
    get_group_permissions,
    get_group_resources,
    get_group_rows,
    get_group_zones,
    get_role,
    get_row_access_profile,
    get_user_by_id,
    get_user_channels,
    get_user_permissions,
    get_user_resources,
    get_user_rows,
    get_user_zones,
    get_users_from_group,
    get_zone_access_profile,
    list_access_profiles,
    list_channel_access_profiles,
    list_channel_access_profiles_for_channel,
    list_channel_policies,
    list_channel_policies_for_channel,
    list_groups,
    list_policies,
    list_policies_for_channel,
    list_policies_for_row,
    list_policies_for_zone,
    list_role_subjects,
    list_roles,
    list_row_access_profiles,
    list_row_access_profiles_for_row,
    list_row_policies,
    list_row_policies_for_row,
    list_users,
    list_zone_access_profiles,
    list_zone_access_profiles_for_zone,
    list_zone_policies,
    list_zone_policies_for_zone,
    patch_channel_access_profile,
    patch_channel_policy,
    patch_group,
    patch_role,
    patch_row_access_profile,
    patch_row_policy,
    patch_user,
    patch_user_by_id,
    patch_zone_access_profile,
    patch_zone_policy,
    remove_role_from_group,
    remove_role_from_user,
    remove_user_from_group,
)
from kronicle.errors.error_types import BadRequestError, NotFoundError
from kronicle.schemas.core.input_ressource_schema import InputCoreChannel, InputRow, InputZonePatch
from kronicle.schemas.payload.input_payload import InputPayload
from kronicle.schemas.rbac.input_group_schemas import InputGroup
from kronicle.schemas.rbac.input_policy_schemas import (
    InputAccessProfilePatch,
    InputChannelAccessProfile,
    InputChannelPolicy,
    InputPolicyPatch,
    InputRowAccessProfile,
    InputRowPolicy,
    InputZoneAccessProfile,
    InputZonePolicy,
)
from kronicle.schemas.rbac.input_role_schemas import InputRole
from kronicle.schemas.rbac.input_subject_schemas import InputSubject
from kronicle.schemas.rbac.input_user_schemas import InputUserPatch


@pytest.fixture
def mock_rbac():
    return MagicMock()


@pytest.fixture
def any_uuid():
    return uuid4()


class TestUserRoutes:
    def test_list_users_no_filter(self, mock_rbac):
        mock_rbac.list_users.return_value = []
        result = list_users(email=None, name=None, orcid=None, include_inactive=False, rbac=mock_rbac)
        mock_rbac.list_users.assert_called_once_with(include_inactive=False)
        assert result == []

    def test_list_users_filter_by_email(self, mock_rbac):
        expected = {"email": "test@test.com", "id": uuid4()}
        mock_rbac.fetch_user_by_email.return_value = expected
        result = list_users(email="test@test.com", name=None, orcid=None, include_inactive=False, rbac=mock_rbac)
        mock_rbac.fetch_user_by_email.assert_called_once_with("test@test.com")
        assert result == expected

    def test_list_users_filter_by_name(self, mock_rbac):
        expected = {"name": "testuser", "id": uuid4()}
        mock_rbac.fetch_user_by_name.return_value = expected
        result = list_users(email=None, name="testuser", orcid=None, include_inactive=False, rbac=mock_rbac)
        mock_rbac.fetch_user_by_name.assert_called_once_with("testuser")
        assert result == expected

    def test_list_users_filter_by_orcid(self, mock_rbac):
        expected = {"orcid": "0000-0001-2345-6789", "id": uuid4()}
        mock_rbac.fetch_user_by_external_id.return_value = expected
        result = list_users(email=None, name=None, orcid="0000-0001-2345-6789", include_inactive=False, rbac=mock_rbac)
        mock_rbac.fetch_user_by_external_id.assert_called_once_with("0000-0001-2345-6789")
        assert result == expected

    def test_list_users_include_inactive(self, mock_rbac):
        mock_rbac.list_users.return_value = []
        expected = []
        result = list_users(email=None, name=None, orcid=None, include_inactive=True, rbac=mock_rbac)
        mock_rbac.list_users.assert_called_once_with(include_inactive=True)
        assert result == expected

    def test_get_user_by_id(self, mock_rbac, any_uuid):
        expected = {"id": any_uuid, "email": "test@test.com"}
        mock_rbac.fetch_user_by_id.return_value = expected
        result = get_user_by_id(user_id=any_uuid, rbac=mock_rbac)
        mock_rbac.fetch_user_by_id.assert_called_once_with(any_uuid)
        assert result == expected

    @patch("kronicle.api.rbac_routes.ProcessedUser.from_input")
    def test_create_user(self, mock_from_input, mock_rbac):
        mock_processed = MagicMock()
        mock_from_input.return_value = mock_processed
        expected = {"id": uuid4(), "email": "test@test.com"}
        mock_rbac.create_user.return_value = expected
        user_in = MagicMock()
        result = create_user(user_in=user_in, rbac=mock_rbac)
        mock_from_input.assert_called_once_with(user_in)
        mock_rbac.create_user.assert_called_once_with(user=mock_processed)
        assert result == expected

    @patch("kronicle.api.rbac_routes.ProcessedUser.from_input")
    def test_patch_user(self, mock_from_input, mock_rbac):
        mock_processed = MagicMock()
        mock_from_input.return_value = mock_processed
        expected = {"id": uuid4(), "email": "patched@test.com"}
        mock_rbac.patch_user.return_value = expected
        user_in = MagicMock()
        result = patch_user(user_in=user_in, rbac=mock_rbac)
        mock_from_input.assert_called_once_with(user_in)
        mock_rbac.patch_user.assert_called_once_with(user=mock_processed)
        assert result == expected

    def test_patch_user_by_id(self, mock_rbac, any_uuid):
        user_patch = InputUserPatch(name="newname", full_name="New Name", orcid="0000-0001-2345-6789")
        expected = {"id": any_uuid, "name": "newname"}
        mock_rbac.patch_user_by_id.return_value = expected
        result = patch_user_by_id(user_id=any_uuid, user_in=user_patch, rbac=mock_rbac)
        mock_rbac.patch_user_by_id.assert_called_once_with(
            any_uuid,
            name="newname",
            full_name="New Name",
            orcid="0000-0001-2345-6789",
        )
        assert result == expected

    def test_patch_user_by_id_no_data(self, mock_rbac, any_uuid):
        with pytest.raises(BadRequestError, match="No update data provided"):
            patch_user_by_id(user_id=any_uuid, user_in=None, rbac=mock_rbac)

    @patch("kronicle.api.rbac_routes.ProcessedUser.from_input")
    def test_delete_user_deactivate(self, mock_from_input, mock_rbac):
        mock_processed = MagicMock()
        mock_from_input.return_value = mock_processed
        expected = {"id": uuid4(), "is_active": False}
        mock_rbac.deactivate_user.return_value = expected
        user_in = MagicMock()
        result = delete_user(user_in=user_in, remove=False, rbac=mock_rbac)
        mock_from_input.assert_called_once_with(user_in)
        mock_rbac.deactivate_user.assert_called_once_with(user=mock_processed)
        assert result == expected

    @patch("kronicle.api.rbac_routes.ProcessedUser.from_input")
    def test_delete_user_remove(self, mock_from_input, mock_rbac):
        mock_processed = MagicMock()
        mock_from_input.return_value = mock_processed
        expected = {"id": uuid4()}
        mock_rbac.remove_user.return_value = expected
        user_in = MagicMock()
        result = delete_user(user_in=user_in, remove=True, rbac=mock_rbac)
        mock_from_input.assert_called_once_with(user_in)
        mock_rbac.remove_user.assert_called_once_with(user=mock_processed)
        assert result == expected

    def test_delete_user_by_id_deactivate(self, mock_rbac, any_uuid):
        expected = {"id": any_uuid}
        mock_rbac.deactivate_user_by_id.return_value = expected
        result = delete_user_by_id(user_id=any_uuid, remove=False, rbac=mock_rbac)
        mock_rbac.deactivate_user_by_id.assert_called_once_with(id=any_uuid)
        assert result == expected

    def test_delete_user_by_id_remove(self, mock_rbac, any_uuid):
        expected = {"id": any_uuid}
        mock_rbac.remove_user_by_id.return_value = expected
        result = delete_user_by_id(user_id=any_uuid, remove=True, rbac=mock_rbac)
        mock_rbac.remove_user_by_id.assert_called_once_with(id=any_uuid)
        assert result == expected


class TestUserRoleRoutes:
    def test_assign_role_to_user(self, mock_rbac, any_uuid):
        role_id = uuid4()
        mock_rbac.assign_role_to_user.return_value = None
        result = assign_role_to_user(user_id=any_uuid, role_id=role_id, rbac=mock_rbac)
        mock_rbac.assign_role_to_user.assert_called_once_with(user_id=any_uuid, role_id=role_id)
        assert result == {"detail": f"Role '{role_id}' assigned to user '{any_uuid}'"}

    def test_remove_role_from_user(self, mock_rbac, any_uuid):
        role_id = uuid4()
        mock_rbac.remove_role_from_user.return_value = None
        result = remove_role_from_user(user_id=any_uuid, role_id=role_id, rbac=mock_rbac)
        mock_rbac.remove_role_from_user.assert_called_once_with(user_id=any_uuid, role_id=role_id)
        assert result == {"detail": f"Role '{role_id}' removed from user '{any_uuid}'"}


class TestGroupRoutes:
    def test_create_group(self, mock_rbac):
        group_in = InputGroup(name="test-group")
        expected = {"id": uuid4(), "name": "test-group"}
        mock_rbac.create_group.return_value = expected
        result = create_group(group_in=group_in, rbac=mock_rbac)
        mock_rbac.create_group.assert_called_once_with(name="test-group", details=None)
        assert result == expected

    def test_list_groups(self, mock_rbac):
        expected = [{"id": uuid4(), "name": "group1"}]
        mock_rbac.get_groups.return_value = expected
        result = list_groups(name=None, rbac=mock_rbac)
        mock_rbac.get_groups.assert_called_once()
        assert result == expected

    def test_list_groups_with_name(self, mock_rbac):
        expected = {"id": uuid4(), "name": "specific-group"}
        mock_rbac.get_group_by_name.return_value = expected
        result = list_groups(name="specific-group", rbac=mock_rbac)
        mock_rbac.get_group_by_name.assert_called_once_with("specific-group")
        assert result == expected

    def test_get_group(self, mock_rbac, any_uuid):
        expected = {"id": any_uuid, "name": "test-group"}
        mock_rbac.get_group_by_id.return_value = expected
        result = get_group(group_id=any_uuid, rbac=mock_rbac)
        mock_rbac.get_group_by_id.assert_called_once_with(any_uuid)
        assert result == expected

    def test_get_group_not_found(self, mock_rbac, any_uuid):
        mock_rbac.get_group_by_id.return_value = None
        with pytest.raises(NotFoundError, match=str(any_uuid)):
            get_group(group_id=any_uuid, rbac=mock_rbac)

    def test_patch_group(self, mock_rbac, any_uuid):
        group_in = InputGroup(name="updated-group", details={"key": "val"})
        expected = {"id": any_uuid, "name": "updated-group"}
        mock_rbac.patch_group.return_value = expected
        result = patch_group(group_id=any_uuid, group_in=group_in, rbac=mock_rbac)
        mock_rbac.patch_group.assert_called_once_with(any_uuid, name="updated-group", details={"key": "val"})
        assert result == expected

    def test_patch_group_no_input(self, mock_rbac, any_uuid):
        expected = {"id": any_uuid, "name": "unchanged"}
        mock_rbac.patch_group.return_value = expected
        result = patch_group(group_id=any_uuid, group_in=None, rbac=mock_rbac)
        mock_rbac.patch_group.assert_called_once_with(any_uuid, name=None, details=None)
        assert result == expected

    def test_delete_group(self, mock_rbac, any_uuid):
        expected = {"id": any_uuid, "name": "deleted-group"}
        mock_rbac.delete_group.return_value = expected
        result = delete_group(group_id=any_uuid, force=False, rbac=mock_rbac)
        mock_rbac.delete_group.assert_called_once_with(any_uuid, force=False)
        assert result == expected

    def test_delete_group_not_found(self, mock_rbac, any_uuid):
        mock_rbac.delete_group.return_value = None
        with pytest.raises(NotFoundError, match=str(any_uuid)):
            delete_group(group_id=any_uuid, rbac=mock_rbac)

    def test_add_user_to_group(self, mock_rbac, any_uuid):
        user_id = uuid4()
        mock_rbac.add_user_to_group.return_value = None
        result = add_user_to_group(group_id=any_uuid, user_id=user_id, rbac=mock_rbac)
        mock_rbac.add_user_to_group.assert_called_once_with(user_id=user_id, group_id=any_uuid)
        assert result == {"detail": f"User '{user_id}' added to group '{any_uuid}'"}

    def test_get_users_from_group(self, mock_rbac, any_uuid):
        expected = [{"id": uuid4(), "email": "user@test.com"}]
        mock_rbac.get_users_from_group.return_value = expected
        result = get_users_from_group(group_id=any_uuid, rbac=mock_rbac)
        mock_rbac.get_users_from_group.assert_called_once_with(group_id=any_uuid)
        assert result == expected

    def test_remove_user_from_group(self, mock_rbac, any_uuid):
        user_id = uuid4()
        mock_rbac.remove_user_from_group.return_value = None
        result = remove_user_from_group(group_id=any_uuid, user_id=user_id, rbac=mock_rbac)
        mock_rbac.remove_user_from_group.assert_called_once_with(user_id=user_id, group_id=any_uuid)
        assert result == {"detail": f"User '{user_id}' removed from group '{any_uuid}'"}


class TestGroupRoleRoutes:
    def test_assign_role_to_group(self, mock_rbac, any_uuid):
        role_id = uuid4()
        mock_rbac.assign_role_to_group.return_value = None
        result = assign_role_to_group(group_id=any_uuid, role_id=role_id, rbac=mock_rbac)
        mock_rbac.assign_role_to_group.assert_called_once_with(group_id=any_uuid, role_id=role_id)
        assert result == {"detail": f"Role '{role_id}' assigned to group '{any_uuid}'"}

    def test_remove_role_from_group(self, mock_rbac, any_uuid):
        role_id = uuid4()
        mock_rbac.remove_role_from_group.return_value = None
        result = remove_role_from_group(group_id=any_uuid, role_id=role_id, rbac=mock_rbac)
        mock_rbac.remove_role_from_group.assert_called_once_with(group_id=any_uuid, role_id=role_id)
        assert result == {"detail": f"Role '{role_id}' removed from group '{any_uuid}'"}


class TestRoleRoutes:
    def test_create_role(self, mock_rbac):
        role_in = InputRole(name="test-role", description="desc", permissions=["zone:read"])
        expected = {"id": uuid4(), "name": "test-role"}
        mock_rbac.create_role.return_value = expected
        result = create_role(role_in=role_in, rbac=mock_rbac)
        mock_rbac.create_role.assert_called_once_with(
            name="test-role",
            description="desc",
            permissions=["zone:read"],
            restrictions=[],
            details=None,
        )
        assert result == expected

    def test_list_roles(self, mock_rbac):
        expected = [{"id": uuid4(), "name": "role1"}, {"id": uuid4(), "name": "role2"}]
        mock_rbac.get_roles.return_value = expected
        result = list_roles(name=None, rbac=mock_rbac)
        mock_rbac.get_roles.assert_called_once()
        assert result == expected

    def test_get_role(self, mock_rbac, any_uuid):
        expected = {"id": any_uuid, "name": "admin"}
        mock_rbac.get_role.return_value = expected
        result = get_role(role_id=any_uuid, rbac=mock_rbac)
        mock_rbac.get_role.assert_called_once_with(any_uuid)
        assert result == expected

    def test_get_role_not_found(self, mock_rbac, any_uuid):
        mock_rbac.get_role.return_value = None
        with pytest.raises(NotFoundError, match=str(any_uuid)):
            get_role(role_id=any_uuid, rbac=mock_rbac)

    def test_patch_role(self, mock_rbac, any_uuid):
        role_in = InputRole(name="updated-role", permissions=["zone:write"])
        expected = {"id": any_uuid, "name": "updated-role"}
        mock_rbac.patch_role.return_value = expected
        result = patch_role(role_id=any_uuid, role_in=role_in, rbac=mock_rbac)
        mock_rbac.patch_role.assert_called_once_with(
            any_uuid,
            name="updated-role",
            description=None,
            permissions=["zone:write"],
            restrictions=[],
            details=None,
        )
        assert result == expected

    def test_patch_role_no_input(self, mock_rbac, any_uuid):
        expected = {"id": any_uuid, "name": "unchanged"}
        mock_rbac.patch_role.return_value = expected
        result = patch_role(role_id=any_uuid, role_in=None, rbac=mock_rbac)
        mock_rbac.patch_role.assert_called_once_with(
            any_uuid,
            name=None,
            description=None,
            permissions=None,
            restrictions=None,
            details=None,
        )
        assert result == expected

    def test_delete_role(self, mock_rbac, any_uuid):
        expected = {"id": any_uuid, "name": "deleted-role"}
        mock_rbac.delete_role.return_value = expected
        result = delete_role(role_id=any_uuid, force=False, rbac=mock_rbac)
        mock_rbac.delete_role.assert_called_once_with(any_uuid, force=False)
        assert result == expected

    def test_delete_role_not_found(self, mock_rbac, any_uuid):
        mock_rbac.delete_role.return_value = None
        with pytest.raises(NotFoundError, match=str(any_uuid)):
            delete_role(role_id=any_uuid, rbac=mock_rbac)


class TestPolicyRoutes:
    def test_create_zone_policy(self, mock_rbac):
        policy_in = InputZonePolicy(
            subject=InputSubject(id=uuid4(), type="user"),
            access_profile=InputZoneAccessProfile(
                role=InputRole(id=uuid4()),
                zone=InputZonePatch(id=uuid4()),
            ),
        )
        expected = {"id": uuid4()}
        mock_rbac.create_zone_policy.return_value = expected
        result = create_zone_policy(policy_in=policy_in, rbac=mock_rbac)
        mock_rbac.create_zone_policy.assert_called_once_with(
            subject=policy_in.subject,
            access_profile=InputZoneAccessProfile(
                role=policy_in.access_profile.role,
                zone=policy_in.access_profile.zone,
            ),
            name=None,
            details=None,
        )
        assert result == expected

    def test_list_zone_policies(self, mock_rbac, any_uuid):
        expected = [{"id": uuid4(), "zone_id": any_uuid}]
        mock_rbac.list_policies_for_zone.return_value = expected
        result = list_policies_for_zone(zone_id=any_uuid, rbac=mock_rbac)
        mock_rbac.list_policies_for_zone.assert_called_once_with(any_uuid)
        assert result == expected

    def test_delete_zone_policy(self, mock_rbac, any_uuid):
        mock_rbac.delete_zone_policy.return_value = None
        result = delete_zone_policy(policy_id=any_uuid, rbac=mock_rbac)
        mock_rbac.delete_zone_policy.assert_called_once_with(any_uuid)
        assert result == {"detail": f"ZonePolicy '{any_uuid}' deleted"}

    def test_create_channel_policy(self, mock_rbac):
        policy_in = InputChannelPolicy(
            subject=InputSubject(id=uuid4(), type="user"),
            access_profile=InputChannelAccessProfile(
                role=InputRole(id=uuid4()),
                channel=InputPayload(id=uuid4()),
            ),
        )
        expected = {"id": uuid4()}
        mock_rbac.create_channel_policy.return_value = expected
        result = create_channel_policy(policy_in=policy_in, rbac=mock_rbac)
        mock_rbac.create_channel_policy.assert_called_once_with(
            subject=policy_in.subject,
            access_profile=InputChannelAccessProfile(
                role=policy_in.access_profile.role,
                channel=policy_in.access_profile.channel,
            ),
        )
        assert result == expected

    def test_list_channel_policies(self, mock_rbac, any_uuid):
        expected = [{"id": uuid4(), "channel_id": any_uuid}]
        mock_rbac.list_policies_for_channel.return_value = expected
        result = list_policies_for_channel(channel_id=any_uuid, rbac=mock_rbac)
        mock_rbac.list_policies_for_channel.assert_called_once_with(any_uuid)
        assert result == expected

    def test_delete_channel_policy(self, mock_rbac, any_uuid):
        mock_rbac.delete_channel_policy.return_value = None
        result = delete_channel_policy(policy_id=any_uuid, rbac=mock_rbac)
        mock_rbac.delete_channel_policy.assert_called_once_with(any_uuid)
        assert result == {"detail": f"ChannelPolicy '{any_uuid}' deleted"}

    def test_patch_zone_policy(self, mock_rbac, any_uuid):
        patch_in = InputPolicyPatch(name="updated", details={"k": "v"})
        expected = {"id": any_uuid, "name": "updated"}
        mock_rbac.patch_zone_policy.return_value = expected
        result = patch_zone_policy(policy_id=any_uuid, patch_in=patch_in, rbac=mock_rbac)
        mock_rbac.patch_zone_policy.assert_called_once_with(any_uuid, name="updated", details={"k": "v"})
        assert result == expected

    def test_patch_zone_policy_no_input(self, mock_rbac, any_uuid):
        expected = {"id": any_uuid}
        mock_rbac.patch_zone_policy.return_value = expected
        result = patch_zone_policy(policy_id=any_uuid, patch_in=None, rbac=mock_rbac)
        mock_rbac.patch_zone_policy.assert_called_once_with(any_uuid, name=None, details=None)
        assert result == expected

    def test_patch_channel_policy(self, mock_rbac, any_uuid):
        patch_in = InputPolicyPatch(name="ch-updated", details={"a": 1})
        expected = {"id": any_uuid, "name": "ch-updated"}
        mock_rbac.patch_channel_policy.return_value = expected
        result = patch_channel_policy(policy_id=any_uuid, patch_in=patch_in, rbac=mock_rbac)
        mock_rbac.patch_channel_policy.assert_called_once_with(any_uuid, name="ch-updated", details={"a": 1})
        assert result == expected

    def test_patch_channel_policy_no_input(self, mock_rbac, any_uuid):
        expected = {"id": any_uuid}
        mock_rbac.patch_channel_policy.return_value = expected
        result = patch_channel_policy(policy_id=any_uuid, patch_in=None, rbac=mock_rbac)
        mock_rbac.patch_channel_policy.assert_called_once_with(any_uuid, name=None, details=None)
        assert result == expected

    def test_create_row_policy(self, mock_rbac):
        policy_in = InputRowPolicy(
            subject=InputSubject(type="user", user_id=uuid4()),
            access_profile=InputRowAccessProfile(
                role=InputRole(id=uuid4()),
                row=InputRow(id=uuid4(), channel=InputCoreChannel(id=uuid4())),
            ),
        )
        expected = {"id": uuid4()}
        mock_rbac.create_row_policy.return_value = expected
        result = create_row_policy(policy_in=policy_in, rbac=mock_rbac)
        mock_rbac.create_row_policy.assert_called_once_with(
            subject=policy_in.subject,
            access_profile=policy_in.access_profile,
            name=None,
            details=None,
        )
        assert result == expected

    def test_list_policies_for_row(self, mock_rbac, any_uuid):
        expected = [{"id": uuid4(), "row_id": any_uuid}]
        mock_rbac.list_policies_for_row.return_value = expected
        result = list_policies_for_row(row_id=any_uuid, rbac=mock_rbac)
        mock_rbac.list_policies_for_row.assert_called_once_with(any_uuid)
        assert result == expected

    def test_delete_row_policy(self, mock_rbac, any_uuid):
        mock_rbac.delete_row_policy.return_value = None
        result = delete_row_policy(policy_id=any_uuid, rbac=mock_rbac)
        mock_rbac.delete_row_policy.assert_called_once_with(any_uuid)
        assert result == {"detail": f"RowPolicy '{any_uuid}' deleted"}

    def test_patch_row_policy(self, mock_rbac, any_uuid):
        patch_in = InputPolicyPatch(name="row-updated", details={"x": 1})
        expected = {"id": any_uuid, "name": "row-updated"}
        mock_rbac.patch_row_policy.return_value = expected
        result = patch_row_policy(policy_id=any_uuid, patch_in=patch_in, rbac=mock_rbac)
        mock_rbac.patch_row_policy.assert_called_once_with(any_uuid, name="row-updated", details={"x": 1})
        assert result == expected

    def test_patch_row_policy_no_input(self, mock_rbac, any_uuid):
        expected = {"id": any_uuid}
        mock_rbac.patch_row_policy.return_value = expected
        result = patch_row_policy(policy_id=any_uuid, patch_in=None, rbac=mock_rbac)
        mock_rbac.patch_row_policy.assert_called_once_with(any_uuid, name=None, details=None)
        assert result == expected


class TestListRolesByName:
    def test_list_roles_with_name(self, mock_rbac):
        expected = {"id": uuid4(), "name": "admin"}
        mock_rbac.get_role_by_name.return_value = expected
        result = list_roles(name="admin", rbac=mock_rbac)
        mock_rbac.get_role_by_name.assert_called_once_with("admin")
        assert result == expected


class TestRelationshipCheckRoutes:
    def test_check_user_role(self, mock_rbac, any_uuid):
        role_id = uuid4()
        expected = {"has_role": True}
        mock_rbac.check_user_has_role.return_value = expected
        result = check_user_role(user_id=any_uuid, role_id=role_id, indirect=False, rbac=mock_rbac)
        mock_rbac.check_user_has_role.assert_called_once_with(user_id=any_uuid, role_id=role_id, indirect=False)
        assert result == expected

    def test_check_user_role_indirect(self, mock_rbac, any_uuid):
        role_id = uuid4()
        expected = {"has_role": True, "via": "group"}
        mock_rbac.check_user_has_role.return_value = expected
        result = check_user_role(user_id=any_uuid, role_id=role_id, indirect=True, rbac=mock_rbac)
        mock_rbac.check_user_has_role.assert_called_once_with(user_id=any_uuid, role_id=role_id, indirect=True)
        assert result == expected

    def test_check_group_role(self, mock_rbac, any_uuid):
        role_id = uuid4()
        expected = {"has_role": True}
        mock_rbac.check_group_has_role.return_value = expected
        result = check_group_role(group_id=any_uuid, role_id=role_id, indirect=False, rbac=mock_rbac)
        mock_rbac.check_group_has_role.assert_called_once_with(group_id=any_uuid, role_id=role_id, indirect=False)
        assert result == expected

    def test_check_group_role_indirect(self, mock_rbac, any_uuid):
        role_id = uuid4()
        expected = {"has_role": True, "via": "parent_group"}
        mock_rbac.check_group_has_role.return_value = expected
        result = check_group_role(group_id=any_uuid, role_id=role_id, indirect=True, rbac=mock_rbac)
        mock_rbac.check_group_has_role.assert_called_once_with(group_id=any_uuid, role_id=role_id, indirect=True)
        assert result == expected

    def test_list_role_subjects(self, mock_rbac, any_uuid):
        expected = {"users": [uuid4()], "groups": []}
        mock_rbac.list_role_subjects.return_value = expected
        result = list_role_subjects(role_id=any_uuid, indirect=False, rbac=mock_rbac)
        mock_rbac.list_role_subjects.assert_called_once_with(role_id=any_uuid, indirect=False)
        assert result == expected

    def test_list_role_subjects_indirect(self, mock_rbac, any_uuid):
        expected = {"users": [uuid4()], "groups": [uuid4()]}
        mock_rbac.list_role_subjects.return_value = expected
        result = list_role_subjects(role_id=any_uuid, indirect=True, rbac=mock_rbac)
        mock_rbac.list_role_subjects.assert_called_once_with(role_id=any_uuid, indirect=True)
        assert result == expected

    def test_check_user_group(self, mock_rbac):
        user_id, group_id = uuid4(), uuid4()
        expected = {"is_member": True}
        mock_rbac.check_user_in_group.return_value = expected
        result = check_user_group(user_id=user_id, group_id=group_id, indirect=False, rbac=mock_rbac)
        mock_rbac.check_user_in_group.assert_called_once_with(user_id=user_id, group_id=group_id, indirect=False)
        assert result == expected

    def test_check_user_group_indirect(self, mock_rbac):
        user_id, group_id = uuid4(), uuid4()
        expected = {"is_member": True, "via": "sub_group"}
        mock_rbac.check_user_in_group.return_value = expected
        result = check_user_group(user_id=user_id, group_id=group_id, indirect=True, rbac=mock_rbac)
        mock_rbac.check_user_in_group.assert_called_once_with(user_id=user_id, group_id=group_id, indirect=True)
        assert result == expected


class TestAccessProfileRoutes:
    def test_list_access_profiles(self, mock_rbac):
        expected = {"zones": [], "channels": [], "rows": []}
        mock_rbac.list_access_profiles.return_value = expected
        result = list_access_profiles(rbac=mock_rbac)
        mock_rbac.list_access_profiles.assert_called_once()
        assert result == expected

    def test_create_zone_access_profile(self, mock_rbac):
        profile_in = InputZoneAccessProfile(
            role=InputRole(id=uuid4()),
            zone=InputZonePatch(id=uuid4()),
            description="test",
        )
        expected = {"id": uuid4()}
        mock_rbac.create_zone_access_profile.return_value = expected
        result = create_zone_access_profile(profile_in=profile_in, rbac=mock_rbac)
        mock_rbac.create_zone_access_profile.assert_called_once_with(profile_in=profile_in)
        assert result == expected

    def test_list_zone_access_profiles(self, mock_rbac):
        expected = [{"id": uuid4(), "name": "zap1"}]
        mock_rbac.list_zone_access_profiles.return_value = expected
        result = list_zone_access_profiles(rbac=mock_rbac)
        mock_rbac.list_zone_access_profiles.assert_called_once()
        assert result == expected

    def test_get_zone_access_profile(self, mock_rbac, any_uuid):
        expected = {"id": any_uuid, "name": "zap"}
        mock_rbac.get_zone_access_profile.return_value = expected
        result = get_zone_access_profile(profile_id=any_uuid, rbac=mock_rbac)
        mock_rbac.get_zone_access_profile.assert_called_once_with(any_uuid)
        assert result == expected

    def test_get_zone_access_profile_not_found(self, mock_rbac, any_uuid):
        mock_rbac.get_zone_access_profile.return_value = None
        with pytest.raises(NotFoundError, match=str(any_uuid)):
            get_zone_access_profile(profile_id=any_uuid, rbac=mock_rbac)

    def test_patch_zone_access_profile(self, mock_rbac, any_uuid):
        patch_in = InputAccessProfilePatch(description="updated", role=InputRole(id=uuid4()))
        expected = {"id": any_uuid, "description": "updated"}
        mock_rbac.patch_zone_access_profile.return_value = expected
        result = patch_zone_access_profile(profile_id=any_uuid, patch_in=patch_in, rbac=mock_rbac)
        mock_rbac.patch_zone_access_profile.assert_called_once_with(
            any_uuid,
            name=None,
            description="updated",
            details=None,
            role=patch_in.role,
        )
        assert result == expected

    def test_patch_zone_access_profile_no_input(self, mock_rbac, any_uuid):
        expected = {"id": any_uuid}
        mock_rbac.patch_zone_access_profile.return_value = expected
        result = patch_zone_access_profile(profile_id=any_uuid, patch_in=None, rbac=mock_rbac)
        mock_rbac.patch_zone_access_profile.assert_called_once_with(
            any_uuid, name=None, description=None, details=None, role=None
        )
        assert result == expected

    def test_delete_zone_access_profile(self, mock_rbac, any_uuid):
        mock_rbac.delete_zone_access_profile.return_value = None
        result = delete_zone_access_profile(profile_id=any_uuid, rbac=mock_rbac)
        mock_rbac.delete_zone_access_profile.assert_called_once_with(any_uuid)
        assert result == {"detail": f"ZoneAccessProfile '{any_uuid}' deleted"}

    def test_create_channel_access_profile(self, mock_rbac):
        profile_in = InputChannelAccessProfile(
            role=InputRole(id=uuid4()),
            channel=InputPayload(id=uuid4()),
            description="test",
        )
        expected = {"id": uuid4()}
        mock_rbac.create_channel_access_profile.return_value = expected
        result = create_channel_access_profile(profile_in=profile_in, rbac=mock_rbac)
        mock_rbac.create_channel_access_profile.assert_called_once_with(profile_in=profile_in)
        assert result == expected

    def test_list_channel_access_profiles(self, mock_rbac):
        expected = [{"id": uuid4(), "name": "cap1"}]
        mock_rbac.list_channel_access_profiles.return_value = expected
        result = list_channel_access_profiles(rbac=mock_rbac)
        mock_rbac.list_channel_access_profiles.assert_called_once()
        assert result == expected

    def test_get_channel_access_profile(self, mock_rbac, any_uuid):
        expected = {"id": any_uuid, "name": "cap"}
        mock_rbac.get_channel_access_profile.return_value = expected
        result = get_channel_access_profile(profile_id=any_uuid, rbac=mock_rbac)
        mock_rbac.get_channel_access_profile.assert_called_once_with(any_uuid)
        assert result == expected

    def test_get_channel_access_profile_not_found(self, mock_rbac, any_uuid):
        mock_rbac.get_channel_access_profile.return_value = None
        with pytest.raises(NotFoundError, match=str(any_uuid)):
            get_channel_access_profile(profile_id=any_uuid, rbac=mock_rbac)

    def test_patch_channel_access_profile(self, mock_rbac, any_uuid):
        patch_in = InputAccessProfilePatch(description="ch-updated", role=InputRole(id=uuid4()))
        expected = {"id": any_uuid, "description": "ch-updated"}
        mock_rbac.patch_channel_access_profile.return_value = expected
        result = patch_channel_access_profile(profile_id=any_uuid, patch_in=patch_in, rbac=mock_rbac)
        mock_rbac.patch_channel_access_profile.assert_called_once_with(
            any_uuid,
            name=None,
            description="ch-updated",
            details=None,
            role=patch_in.role,
        )
        assert result == expected

    def test_patch_channel_access_profile_no_input(self, mock_rbac, any_uuid):
        expected = {"id": any_uuid}
        mock_rbac.patch_channel_access_profile.return_value = expected
        result = patch_channel_access_profile(profile_id=any_uuid, patch_in=None, rbac=mock_rbac)
        mock_rbac.patch_channel_access_profile.assert_called_once_with(
            any_uuid, name=None, description=None, details=None, role=None
        )
        assert result == expected

    def test_delete_channel_access_profile(self, mock_rbac, any_uuid):
        mock_rbac.delete_channel_access_profile.return_value = None
        result = delete_channel_access_profile(profile_id=any_uuid, rbac=mock_rbac)
        mock_rbac.delete_channel_access_profile.assert_called_once_with(any_uuid)
        assert result == {"detail": f"ChannelAccessProfile '{any_uuid}' deleted"}

    def test_create_row_access_profile(self, mock_rbac):
        profile_in = InputRowAccessProfile(
            role=InputRole(id=uuid4()),
            row=InputRow(id=uuid4(), channel=InputCoreChannel(id=uuid4())),
            description="test",
        )
        expected = {"id": uuid4()}
        mock_rbac.create_row_access_profile.return_value = expected
        result = create_row_access_profile(profile_in=profile_in, rbac=mock_rbac)
        mock_rbac.create_row_access_profile.assert_called_once_with(profile_in=profile_in)
        assert result == expected

    def test_list_row_access_profiles(self, mock_rbac):
        expected = [{"id": uuid4(), "name": "rap1"}]
        mock_rbac.list_row_access_profiles.return_value = expected
        result = list_row_access_profiles(rbac=mock_rbac)
        mock_rbac.list_row_access_profiles.assert_called_once()
        assert result == expected

    def test_get_row_access_profile(self, mock_rbac, any_uuid):
        expected = {"id": any_uuid, "name": "rap"}
        mock_rbac.get_row_access_profile.return_value = expected
        result = get_row_access_profile(profile_id=any_uuid, rbac=mock_rbac)
        mock_rbac.get_row_access_profile.assert_called_once_with(any_uuid)
        assert result == expected

    def test_get_row_access_profile_not_found(self, mock_rbac, any_uuid):
        mock_rbac.get_row_access_profile.return_value = None
        with pytest.raises(NotFoundError, match=str(any_uuid)):
            get_row_access_profile(profile_id=any_uuid, rbac=mock_rbac)

    def test_patch_row_access_profile(self, mock_rbac, any_uuid):
        patch_in = InputAccessProfilePatch(description="row-updated", role=InputRole(id=uuid4()))
        expected = {"id": any_uuid, "description": "row-updated"}
        mock_rbac.patch_row_access_profile.return_value = expected
        result = patch_row_access_profile(profile_id=any_uuid, patch_in=patch_in, rbac=mock_rbac)
        mock_rbac.patch_row_access_profile.assert_called_once_with(
            any_uuid,
            name=None,
            description="row-updated",
            details=None,
            role=patch_in.role,
        )
        assert result == expected

    def test_patch_row_access_profile_no_input(self, mock_rbac, any_uuid):
        expected = {"id": any_uuid}
        mock_rbac.patch_row_access_profile.return_value = expected
        result = patch_row_access_profile(profile_id=any_uuid, patch_in=None, rbac=mock_rbac)
        mock_rbac.patch_row_access_profile.assert_called_once_with(
            any_uuid, name=None, description=None, details=None, role=None
        )
        assert result == expected

    def test_delete_row_access_profile(self, mock_rbac, any_uuid):
        mock_rbac.delete_row_access_profile.return_value = None
        result = delete_row_access_profile(profile_id=any_uuid, rbac=mock_rbac)
        mock_rbac.delete_row_access_profile.assert_called_once_with(any_uuid)
        assert result == {"detail": f"RowAccessProfile '{any_uuid}' deleted"}


class TestListPoliciesGlobal:
    def test_list_policies(self, mock_rbac):
        expected = {"zone": [], "channel": [], "row": []}
        mock_rbac.list_policies.return_value = expected
        result = list_policies(rbac=mock_rbac)
        mock_rbac.list_policies.assert_called_once()
        assert result == expected

    def test_list_zone_policies(self, mock_rbac):
        expected = [{"id": uuid4(), "name": "zp1"}]
        mock_rbac.list_zone_policies.return_value = expected
        result = list_zone_policies(rbac=mock_rbac)
        mock_rbac.list_zone_policies.assert_called_once()
        assert result == expected

    def test_list_channel_policies(self, mock_rbac):
        expected = [{"id": uuid4(), "name": "cp1"}]
        mock_rbac.list_channel_policies.return_value = expected
        result = list_channel_policies(rbac=mock_rbac)
        mock_rbac.list_channel_policies.assert_called_once()
        assert result == expected

    def test_list_row_policies(self, mock_rbac):
        expected = [{"id": uuid4(), "name": "rp1"}]
        mock_rbac.list_row_policies.return_value = expected
        result = list_row_policies(rbac=mock_rbac)
        mock_rbac.list_row_policies.assert_called_once()
        assert result == expected


class TestIntrospectionUserRoutes:
    def test_get_user_permissions(self, mock_rbac, any_uuid):
        expected = {"direct_roles": [], "group_roles": [], "zone_policies": []}
        mock_rbac.get_user_permissions.return_value = expected
        result = get_user_permissions(user_id=any_uuid, rbac=mock_rbac)
        mock_rbac.get_user_permissions.assert_called_once_with(any_uuid)
        assert result == expected

    def test_get_user_zones(self, mock_rbac, any_uuid):
        expected = [{"zone_id": uuid4(), "provenance": "direct"}]
        mock_rbac.get_user_zones.return_value = expected
        result = get_user_zones(user_id=any_uuid, indirect=False, rbac=mock_rbac)
        mock_rbac.get_user_zones.assert_called_once_with(any_uuid, indirect=False)
        assert result == expected

    def test_get_user_zones_indirect(self, mock_rbac, any_uuid):
        expected = [{"zone_id": uuid4(), "provenance": "ancestor"}]
        mock_rbac.get_user_zones.return_value = expected
        result = get_user_zones(user_id=any_uuid, indirect=True, rbac=mock_rbac)
        mock_rbac.get_user_zones.assert_called_once_with(any_uuid, indirect=True)
        assert result == expected

    def test_get_user_channels(self, mock_rbac, any_uuid):
        expected = [{"channel_id": uuid4(), "provenance": "direct"}]
        mock_rbac.get_user_channels.return_value = expected
        result = get_user_channels(user_id=any_uuid, indirect=False, rbac=mock_rbac)
        mock_rbac.get_user_channels.assert_called_once_with(any_uuid, indirect=False)
        assert result == expected

    def test_get_user_channels_indirect(self, mock_rbac, any_uuid):
        expected = [{"channel_id": uuid4(), "provenance": "zone_parent"}]
        mock_rbac.get_user_channels.return_value = expected
        result = get_user_channels(user_id=any_uuid, indirect=True, rbac=mock_rbac)
        mock_rbac.get_user_channels.assert_called_once_with(any_uuid, indirect=True)
        assert result == expected

    def test_get_user_rows(self, mock_rbac, any_uuid):
        expected = [{"row_id": uuid4(), "provenance": "direct"}]
        mock_rbac.get_user_rows.return_value = expected
        result = get_user_rows(user_id=any_uuid, indirect=False, rbac=mock_rbac)
        mock_rbac.get_user_rows.assert_called_once_with(any_uuid, indirect=False)
        assert result == expected

    def test_get_user_resources(self, mock_rbac, any_uuid):
        expected = {"zones": [], "channels": [], "rows": []}
        mock_rbac.get_user_resources.return_value = expected
        result = get_user_resources(user_id=any_uuid, indirect=False, rbac=mock_rbac)
        mock_rbac.get_user_resources.assert_called_once_with(any_uuid, indirect=False)
        assert result == expected

    def test_get_user_resources_indirect(self, mock_rbac, any_uuid):
        expected = {"zones": [], "channels": [], "rows": []}
        mock_rbac.get_user_resources.return_value = expected
        result = get_user_resources(user_id=any_uuid, indirect=True, rbac=mock_rbac)
        mock_rbac.get_user_resources.assert_called_once_with(any_uuid, indirect=True)
        assert result == expected


class TestIntrospectionGroupRoutes:
    def test_get_group_permissions(self, mock_rbac, any_uuid):
        expected = {"direct_roles": [], "group_roles": [], "zone_policies": []}
        mock_rbac.get_group_permissions.return_value = expected
        result = get_group_permissions(group_id=any_uuid, rbac=mock_rbac)
        mock_rbac.get_group_permissions.assert_called_once_with(any_uuid)
        assert result == expected

    def test_get_group_zones(self, mock_rbac, any_uuid):
        expected = [{"zone_id": uuid4(), "provenance": "direct"}]
        mock_rbac.get_group_zones.return_value = expected
        result = get_group_zones(group_id=any_uuid, indirect=False, rbac=mock_rbac)
        mock_rbac.get_group_zones.assert_called_once_with(any_uuid, indirect=False)
        assert result == expected

    def test_get_group_zones_indirect(self, mock_rbac, any_uuid):
        expected = [{"zone_id": uuid4(), "provenance": "ancestor"}]
        mock_rbac.get_group_zones.return_value = expected
        result = get_group_zones(group_id=any_uuid, indirect=True, rbac=mock_rbac)
        mock_rbac.get_group_zones.assert_called_once_with(any_uuid, indirect=True)
        assert result == expected

    def test_get_group_channels(self, mock_rbac, any_uuid):
        expected = [{"channel_id": uuid4(), "provenance": "direct"}]
        mock_rbac.get_group_channels.return_value = expected
        result = get_group_channels(group_id=any_uuid, indirect=False, rbac=mock_rbac)
        mock_rbac.get_group_channels.assert_called_once_with(any_uuid, indirect=False)
        assert result == expected

    def test_get_group_channels_indirect(self, mock_rbac, any_uuid):
        expected = [{"channel_id": uuid4(), "provenance": "zone_parent"}]
        mock_rbac.get_group_channels.return_value = expected
        result = get_group_channels(group_id=any_uuid, indirect=True, rbac=mock_rbac)
        mock_rbac.get_group_channels.assert_called_once_with(any_uuid, indirect=True)
        assert result == expected

    def test_get_group_rows(self, mock_rbac, any_uuid):
        expected = [{"row_id": uuid4(), "provenance": "direct"}]
        mock_rbac.get_group_rows.return_value = expected
        result = get_group_rows(group_id=any_uuid, indirect=False, rbac=mock_rbac)
        mock_rbac.get_group_rows.assert_called_once_with(any_uuid, indirect=False)
        assert result == expected

    def test_get_group_resources(self, mock_rbac, any_uuid):
        expected = {"zones": [], "channels": [], "rows": []}
        mock_rbac.get_group_resources.return_value = expected
        result = get_group_resources(group_id=any_uuid, indirect=False, rbac=mock_rbac)
        mock_rbac.get_group_resources.assert_called_once_with(any_uuid, indirect=False)
        assert result == expected

    def test_get_group_resources_indirect(self, mock_rbac, any_uuid):
        expected = {"zones": [], "channels": [], "rows": []}
        mock_rbac.get_group_resources.return_value = expected
        result = get_group_resources(group_id=any_uuid, indirect=True, rbac=mock_rbac)
        mock_rbac.get_group_resources.assert_called_once_with(any_uuid, indirect=True)
        assert result == expected


class TestResourceLevelPolicyRoutes:
    def test_list_zone_policies_for_zone(self, mock_rbac, any_uuid):
        expected = [{"id": uuid4(), "zone_id": any_uuid}]
        mock_rbac.get_zone_policies.return_value = expected
        result = list_zone_policies_for_zone(zone_id=any_uuid, rbac=mock_rbac)
        mock_rbac.get_zone_policies.assert_called_once_with(any_uuid)
        assert result == expected

    def test_list_zone_access_profiles_for_zone(self, mock_rbac, any_uuid):
        expected = [{"id": uuid4(), "zone_id": any_uuid}]
        mock_rbac.get_zone_access_profiles.return_value = expected
        result = list_zone_access_profiles_for_zone(zone_id=any_uuid, rbac=mock_rbac)
        mock_rbac.get_zone_access_profiles.assert_called_once_with(any_uuid)
        assert result == expected

    def test_list_channel_policies_for_channel(self, mock_rbac, any_uuid):
        expected = [{"id": uuid4(), "channel_id": any_uuid}]
        mock_rbac.get_channel_policies.return_value = expected
        result = list_channel_policies_for_channel(channel_id=any_uuid, rbac=mock_rbac)
        mock_rbac.get_channel_policies.assert_called_once_with(any_uuid)
        assert result == expected

    def test_list_channel_access_profiles_for_channel(self, mock_rbac, any_uuid):
        expected = [{"id": uuid4(), "channel_id": any_uuid}]
        mock_rbac.get_channel_access_profiles.return_value = expected
        result = list_channel_access_profiles_for_channel(channel_id=any_uuid, rbac=mock_rbac)
        mock_rbac.get_channel_access_profiles.assert_called_once_with(any_uuid)
        assert result == expected

    def test_list_row_policies_for_row(self, mock_rbac, any_uuid):
        expected = [{"id": uuid4(), "row_id": any_uuid}]
        mock_rbac.get_row_policies.return_value = expected
        result = list_row_policies_for_row(row_id=any_uuid, rbac=mock_rbac)
        mock_rbac.get_row_policies.assert_called_once_with(any_uuid)
        assert result == expected

    def test_list_row_access_profiles_for_row(self, mock_rbac, any_uuid):
        expected = [{"id": uuid4(), "row_id": any_uuid}]
        mock_rbac.get_row_access_profiles.return_value = expected
        result = list_row_access_profiles_for_row(row_id=any_uuid, rbac=mock_rbac)
        mock_rbac.get_row_access_profiles.assert_called_once_with(any_uuid)
        assert result == expected
