from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from kronicle.api.rbac_routes import (
    add_user_to_group,
    assign_role_to_group,
    assign_role_to_user,
    create_channel_policy,
    create_group,
    create_role,
    create_user,
    create_zone_policy,
    delete_channel_policy,
    delete_group,
    delete_role,
    delete_user,
    delete_user_by_id,
    delete_zone_policy,
    get_group,
    get_role,
    get_user_by_id,
    get_users_from_group,
    list_channel_policies,
    list_groups,
    list_roles,
    list_users,
    list_zone_policies,
    patch_group,
    patch_role,
    patch_user,
    patch_user_by_id,
    remove_role_from_group,
    remove_role_from_user,
    remove_user_from_group,
)
from kronicle.errors.error_types import BadRequestError, NotFoundError
from kronicle.schemas.rbac.input_group_schemas import InputGroup
from kronicle.schemas.rbac.input_policy_schemas import InputChannelPolicy, InputZonePolicy
from kronicle.schemas.rbac.input_role_schemas import InputRole
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
        result = list_users(email=None, name=None, orcid=None, include_inactive=True, rbac=mock_rbac)
        mock_rbac.list_users.assert_called_once_with(include_inactive=True)

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
        mock_rbac.create_group.assert_called_once_with(name="test-group", details={})
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
            details={},
        )
        assert result == expected

    def test_list_roles(self, mock_rbac):
        expected = [{"id": uuid4(), "name": "role1"}, {"id": uuid4(), "name": "role2"}]
        mock_rbac.get_roles.return_value = expected
        result = list_roles(rbac=mock_rbac)
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
            details={},
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
        policy_in = InputZonePolicy(subject_id=uuid4(), role_id=uuid4(), zone_id=uuid4())
        expected = {"id": uuid4()}
        mock_rbac.create_zone_policy.return_value = expected
        result = create_zone_policy(policy_in=policy_in, rbac=mock_rbac)
        mock_rbac.create_zone_policy.assert_called_once_with(
            subject_id=policy_in.subject_id,
            role_id=policy_in.role_id,
            zone_id=policy_in.zone_id,
        )
        assert result == expected

    def test_list_zone_policies(self, mock_rbac, any_uuid):
        expected = [{"id": uuid4(), "zone_id": any_uuid}]
        mock_rbac.list_zone_policies.return_value = expected
        result = list_zone_policies(zone_id=any_uuid, rbac=mock_rbac)
        mock_rbac.list_zone_policies.assert_called_once_with(any_uuid)
        assert result == expected

    def test_delete_zone_policy(self, mock_rbac, any_uuid):
        mock_rbac.delete_zone_policy.return_value = None
        result = delete_zone_policy(policy_id=any_uuid, rbac=mock_rbac)
        mock_rbac.delete_zone_policy.assert_called_once_with(any_uuid)
        assert result == {"detail": f"ZonePolicy '{any_uuid}' deleted"}

    def test_create_channel_policy(self, mock_rbac):
        policy_in = InputChannelPolicy(subject_id=uuid4(), role_id=uuid4(), channel_id=uuid4())
        expected = {"id": uuid4()}
        mock_rbac.create_channel_policy.return_value = expected
        result = create_channel_policy(policy_in=policy_in, rbac=mock_rbac)
        mock_rbac.create_channel_policy.assert_called_once_with(
            subject_id=policy_in.subject_id,
            role_id=policy_in.role_id,
            channel_id=policy_in.channel_id,
        )
        assert result == expected

    def test_list_channel_policies(self, mock_rbac, any_uuid):
        expected = [{"id": uuid4(), "channel_id": any_uuid}]
        mock_rbac.list_channel_policies.return_value = expected
        result = list_channel_policies(channel_id=any_uuid, rbac=mock_rbac)
        mock_rbac.list_channel_policies.assert_called_once_with(any_uuid)
        assert result == expected

    def test_delete_channel_policy(self, mock_rbac, any_uuid):
        mock_rbac.delete_channel_policy.return_value = None
        result = delete_channel_policy(policy_id=any_uuid, rbac=mock_rbac)
        mock_rbac.delete_channel_policy.assert_called_once_with(any_uuid)
        assert result == {"detail": f"ChannelPolicy '{any_uuid}' deleted"}
