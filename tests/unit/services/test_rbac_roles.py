# tests/unit/services/test_rbac_roles.py
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from kronicle.errors.error_types import BadRequestError, ConflictError, NotFoundError
from kronicle.schemas.rbac.safe_role_schemas import OutputRole
from tests.unit.services.conftest import fake_group, fake_group_role_link, fake_role, fake_user, fake_user_role_link


class TestRoles:
    @patch("kronicle.services.rbac_service.RbacRole")
    def test_create_role(self, mock_rbac_role, rbac_service):
        mock_role = MagicMock()
        mock_role.id = uuid4()
        mock_role.name = "test-role"
        mock_role.description = "desc"
        mock_role.permissions = ["zone:read"]
        mock_role.restrictions = []
        mock_role.details = {"k": "v"}
        mock_rbac_role.return_value = mock_role
        rbac_service._role_repo.get_by_name = MagicMock(return_value=None)
        out = rbac_service.create_role("test-role", description="desc", permissions=["zone:read"], details={"k": "v"})
        assert isinstance(out, OutputRole)
        assert out.name == "test-role"
        assert out.permissions == ["zone:read"]

    def test_create_role_duplicate(self, rbac_service):
        rbac_service._role_repo.get_by_name = MagicMock(return_value=fake_role(name="dup"))
        with pytest.raises(BadRequestError, match="already exists"):
            rbac_service.create_role("dup")

    def test_get_roles(self, rbac_service):
        rbac_service._role_repo.fetch_all = MagicMock(return_value=[fake_role(), fake_role()])
        result = rbac_service.get_roles()
        assert len(result) == 2
        assert all(isinstance(r, OutputRole) for r in result)

    def test_get_role(self, rbac_service):
        rid = uuid4()
        rbac_service._role_repo.get_by_id = MagicMock(return_value=fake_role(id=rid))
        result = rbac_service.get_role(rid)
        assert isinstance(result, OutputRole)
        assert result.id == rid

    def test_get_role_none(self, rbac_service):
        rbac_service._role_repo.get_by_id = MagicMock(return_value=None)
        assert rbac_service.get_role(uuid4()) is None

    def test_get_role_by_name(self, rbac_service):
        rbac_service._role_repo.get_by_name = MagicMock(return_value=fake_role(name="found"))
        result = rbac_service.get_role_by_name("found")
        assert isinstance(result, OutputRole)
        assert result.name == "found"

    def test_get_role_by_name_none(self, rbac_service):
        rbac_service._role_repo.get_by_name = MagicMock(return_value=None)
        assert rbac_service.get_role_by_name("noone") is None

    def test_patch_role(self, rbac_service):
        rid = uuid4()
        role = fake_role(id=rid, name="old")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)

        out = rbac_service.patch_role(
            rid,
            name="new",
            description="new desc",
            permissions=["zone:write"],
            restrictions=["zone:delete"],
            details={"k": "v"},
        )
        assert out.name == "new"
        assert role.name == "new"
        assert role.description == "new desc"

    def test_patch_role_not_found(self, rbac_service):
        rbac_service._role_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.patch_role(uuid4(), name="x")

    def test_patch_role_partial(self, rbac_service):
        rid = uuid4()
        role = fake_role(id=rid, name="old")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)

        out = rbac_service.patch_role(rid, name="partial")
        assert out.name == "partial"

    def test_delete_role_force(self, rbac_service):
        rid = uuid4()
        role = fake_role(id=rid, name="del")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        out = rbac_service.delete_role(rid, force=True)
        assert isinstance(out, OutputRole)

    def test_delete_role_not_found(self, rbac_service):
        rbac_service._role_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.delete_role(uuid4())

    def test_delete_role_with_users(self, rbac_service):
        rid = uuid4()
        role = fake_role(id=rid, name="del")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        user = fake_user()
        db.execute.side_effect = [
            MagicMock(scalars=lambda: MagicMock(all=lambda: [user])),
            MagicMock(scalars=lambda: MagicMock(all=lambda: [])),
        ]
        with pytest.raises(ConflictError, match="cannot be deleted"):
            rbac_service.delete_role(rid, force=False)

    def test_delete_role_with_groups(self, rbac_service):
        rid = uuid4()
        role = fake_role(id=rid, name="del")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        db.execute.side_effect = [
            MagicMock(scalars=lambda: MagicMock(all=lambda: [])),
            MagicMock(scalars=lambda: MagicMock(all=lambda: [fake_group()])),
        ]
        with pytest.raises(ConflictError, match="cannot be deleted"):
            rbac_service.delete_role(rid, force=False)


# ==================================================================================================
# User ↔ Role + Group ↔ Role assignments
# ==================================================================================================


class TestAssignments:
    def test_assign_role_to_user(self, rbac_service):
        uid, rid = uuid4(), uuid4()
        link = fake_user_role_link(user=fake_user(id=uid), role=fake_role(id=rid))
        rbac_service._user_roles_repo.assign_role_to_user = MagicMock(return_value=link)
        result = rbac_service.assign_role_to_user(uid, rid)
        assert result is not None
        assert result.user.id == uid
        assert result.role.id == rid

    def test_remove_role_from_user(self, rbac_service):
        uid, rid = uuid4(), uuid4()
        link = fake_user_role_link(user=fake_user(id=uid), role=fake_role(id=rid))
        rbac_service._user_roles_repo.remove_role_from_user = MagicMock(return_value=link)
        result = rbac_service.remove_role_from_user(uid, rid)
        assert result is not None
        assert result.user.id == uid
        assert result.role.id == rid

    def test_assign_role_to_group(self, rbac_service):
        gid, rid = uuid4(), uuid4()
        link = fake_group_role_link(group=fake_group(id=gid), role=fake_role(id=rid))
        rbac_service._group_roles_repo.assign_role_to_group = MagicMock(return_value=link)
        result = rbac_service.assign_role_to_group(gid, rid)
        assert result is not None
        assert result.group.id == gid
        assert result.role.id == rid

    def test_remove_role_from_group(self, rbac_service):
        gid, rid = uuid4(), uuid4()
        link = fake_group_role_link(group=fake_group(id=gid), role=fake_role(id=rid))
        rbac_service._group_roles_repo.remove_role_from_group = MagicMock(return_value=link)
        result = rbac_service.remove_role_from_group(gid, rid)
        assert result is not None
        assert result.group.id == gid
        assert result.role.id == rid


# ==================================================================================================
# Permissions
# ==================================================================================================


class TestUserHasRole:
    def test_direct(self, rbac_service):
        uid, rid = uuid4(), uuid4()
        rbac_service._user_repo.get_by_id = MagicMock(return_value=fake_user(id=uid))
        rbac_service._role_repo.get_by_id = MagicMock(return_value=fake_role(id=rid))
        link = fake_user_role_link(user=fake_user(id=uid), role=fake_role(id=rid))
        rbac_service._user_roles_repo.check_link = MagicMock(return_value=link)

        result = rbac_service.check_user_has_role(uid, rid)
        assert result is not None
        assert result.user.id == uid
        assert result.role.id == rid

    def test_not_direct_no_indirect(self, rbac_service):
        uid, rid = uuid4(), uuid4()
        rbac_service._user_repo.get_by_id = MagicMock(return_value=fake_user(id=uid))
        rbac_service._role_repo.get_by_id = MagicMock(return_value=fake_role(id=rid))
        rbac_service._user_roles_repo.check_link = MagicMock(return_value=None)

        result = rbac_service.check_user_has_role(uid, rid, indirect=False)
        assert result is None

    def test_via_group(self, rbac_service):
        uid, rid, gid = uuid4(), uuid4(), uuid4()
        rbac_service._user_repo.get_by_id = MagicMock(return_value=fake_user(id=uid))
        rbac_service._role_repo.get_by_id = MagicMock(return_value=fake_role(id=rid))
        rbac_service._user_roles_repo.check_link = MagicMock(return_value=None)
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value={gid})
        rbac_service._get_group_ancestor_ids = MagicMock(return_value={gid})
        group_role = fake_group_role_link(group=fake_group(id=gid), role=fake_role(id=rid))
        rbac_service._group_roles_repo.get_role_link_for_groups = MagicMock(return_value=group_role)

        result = rbac_service.check_user_has_role(uid, rid, indirect=True)
        assert result is not None
        assert result.group.id == gid
        assert result.role.id == rid

    def test_not_found_indirect(self, rbac_service):
        uid, rid = uuid4(), uuid4()
        rbac_service._user_repo.get_by_id = MagicMock(return_value=fake_user(id=uid))
        rbac_service._role_repo.get_by_id = MagicMock(return_value=fake_role(id=rid))
        rbac_service._user_roles_repo.check_link = MagicMock(return_value=None)
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value=set())
        rbac_service._group_roles_repo.get_role_link_for_groups = MagicMock(return_value=None)

        result = rbac_service.check_user_has_role(uid, rid, indirect=True)
        assert result is None

    def test_via_group_with_ancestors(self, rbac_service):
        uid, rid, gid, aid = uuid4(), uuid4(), uuid4(), uuid4()
        rbac_service._user_repo.get_by_id = MagicMock(return_value=fake_user(id=uid))
        rbac_service._role_repo.get_by_id = MagicMock(return_value=fake_role(id=rid))
        rbac_service._user_roles_repo.check_link = MagicMock(return_value=None)
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value={gid})
        rbac_service._get_group_ancestor_ids = MagicMock(return_value={aid})
        group_role = fake_group_role_link(group=fake_group(id=aid), role=fake_role(id=rid))
        rbac_service._group_roles_repo.get_role_link_for_groups = MagicMock(return_value=group_role)

        result = rbac_service.check_user_has_role(uid, rid, indirect=True)
        assert result is not None
        assert result.group.id == aid


class TestGroupHasRole:
    def test_direct(self, rbac_service):
        gid, rid = uuid4(), uuid4()
        rbac_service._group_repo.get_by_id = MagicMock(return_value=fake_group(id=gid))
        rbac_service._role_repo.get_by_id = MagicMock(return_value=fake_role(id=rid))
        link = fake_group_role_link(group=fake_group(id=gid), role=fake_role(id=rid))
        rbac_service._group_roles_repo.check_link = MagicMock(return_value=link)

        result = rbac_service.check_group_has_role(gid, rid)
        assert result is not None
        assert result.group.id == gid
        assert result.role.id == rid

    def test_not_direct_no_indirect(self, rbac_service):
        gid, rid = uuid4(), uuid4()
        rbac_service._group_repo.get_by_id = MagicMock(return_value=fake_group(id=gid))
        rbac_service._role_repo.get_by_id = MagicMock(return_value=fake_role(id=rid))
        rbac_service._group_roles_repo.check_link = MagicMock(return_value=None)

        result = rbac_service.check_group_has_role(gid, rid, indirect=False)
        assert result is None

    def test_via_ancestor(self, rbac_service):
        gid, rid, aid = uuid4(), uuid4(), uuid4()
        rbac_service._group_repo.get_by_id = MagicMock(side_effect=lambda db, id: fake_group(id=id))
        rbac_service._role_repo.get_by_id = MagicMock(return_value=fake_role(id=rid))
        rbac_service._group_roles_repo.check_link = MagicMock(return_value=None)
        rbac_service._get_group_ancestor_ids = MagicMock(return_value={aid})
        group_role = fake_group_role_link(group=fake_group(id=aid), role=fake_role(id=rid))
        rbac_service._group_roles_repo.get_role_link_for_groups = MagicMock(return_value=group_role)

        result = rbac_service.check_group_has_role(gid, rid, indirect=True)
        assert result is not None
        assert result.group.id == aid


class TestListRoleSubjects:
    def test_direct(self, rbac_service):
        rid = uuid4()
        uid1, uid2, gid = uuid4(), uuid4(), uuid4()
        rbac_service._user_repo.get_by_id = MagicMock(side_effect=lambda db, id: fake_user(id=id))
        rbac_service._group_repo.get_by_id = MagicMock(side_effect=lambda db, id: fake_group(id=id))
        rbac_service._user_roles_repo.get_user_ids_for_role = MagicMock(return_value={uid1, uid2})
        rbac_service._group_roles_repo.get_group_ids_for_role = MagicMock(return_value={gid})

        result = rbac_service.list_role_subjects(rid, indirect=False)
        assert len(result.users) == 2
        user_ids = {u.id for u in result.users}
        assert uid1 in user_ids
        assert uid2 in user_ids
        assert len(result.groups) == 1
        assert result.groups[0].id == gid

    def test_indirect(self, rbac_service):
        rid = uuid4()
        uid, gid, member_uid = uuid4(), uuid4(), uuid4()
        rbac_service._user_repo.get_by_id = MagicMock(side_effect=lambda db, id: fake_user(id=id))
        rbac_service._group_repo.get_by_id = MagicMock(side_effect=lambda db, id: fake_group(id=id))
        rbac_service._user_roles_repo.get_user_ids_for_role = MagicMock(return_value={uid})
        rbac_service._group_roles_repo.get_group_ids_for_role = MagicMock(return_value={gid})
        rbac_service._user_groups_repo.get_user_ids_for_group = MagicMock(return_value={member_uid})
        rbac_service._get_group_descendant_ids = MagicMock(return_value=set())

        result = rbac_service.list_role_subjects(rid, indirect=True)
        assert len(result.users) == 1
        assert result.users[0].id == uid
        assert len(result.groups) == 1
        assert result.groups[0].id == gid
        assert len(result.indirect_users) == 1
        assert result.indirect_users[0].id == member_uid


class TestListRoleSubjectsDescendants:
    def test_indirect_with_descendants(self, rbac_service):
        rid, uid, gid, desc_gid, member_uid = uuid4(), uuid4(), uuid4(), uuid4(), uuid4()
        rbac_service._user_repo.get_by_id = MagicMock(side_effect=lambda db, id: fake_user(id=id))
        rbac_service._group_repo.get_by_id = MagicMock(side_effect=lambda db, id: fake_group(id=id))
        rbac_service._user_roles_repo.get_user_ids_for_role = MagicMock(return_value={uid})
        rbac_service._group_roles_repo.get_group_ids_for_role = MagicMock(return_value={gid})
        rbac_service._user_groups_repo.get_user_ids_for_group = MagicMock(return_value={member_uid})
        rbac_service._get_group_descendant_ids = MagicMock(return_value={desc_gid})

        result = rbac_service.list_role_subjects(rid, indirect=True)
        assert len(result.users) == 1
        assert result.users[0].id == uid
        assert len(result.groups) == 1
        assert result.groups[0].id == gid


# ==================================================================================================
# _check_policy_perm group match (line 421)
# ==================================================================================================
