# tests/unit/services/test_rbac_roles.py
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from kronicle.errors.error_types import BadRequestError, ConflictError, NotFoundError
from kronicle.schemas.rbac.safe_role_schemas import OutputRole
from tests.unit.services.conftest import fake_group, fake_role, fake_user


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
        rbac_service.assign_role_to_user(uuid4(), uuid4())
        assert rbac_service._db.transaction.return_value.__enter__.return_value.execute.called

    def test_remove_role_from_user(self, rbac_service):
        rbac_service.remove_role_from_user(uuid4(), uuid4())
        assert rbac_service._db.transaction.return_value.__enter__.return_value.execute.called

    def test_assign_role_to_group(self, rbac_service):
        rbac_service.assign_role_to_group(uuid4(), uuid4())
        assert rbac_service._db.transaction.return_value.__enter__.return_value.execute.called

    def test_remove_role_from_group(self, rbac_service):
        rbac_service.remove_role_from_group(uuid4(), uuid4())
        assert rbac_service._db.transaction.return_value.__enter__.return_value.execute.called


# ==================================================================================================
# Permissions
# ==================================================================================================


class TestUserHasRole:
    def test_direct(self, rbac_service):
        uid, rid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.return_value = (uid, rid)

        result = rbac_service.check_user_has_role(uid, rid)
        assert result == {"has_role": True, "direct": True}

    def test_not_direct_no_indirect(self, rbac_service):
        uid, rid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.return_value = None

        result = rbac_service.check_user_has_role(uid, rid, indirect=False)
        assert result == {"has_role": False, "direct": False}

    def test_via_group(self, rbac_service):
        uid, rid, gid = uuid4(), uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.side_effect = [None, (gid, rid)]
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value={gid})

        result = rbac_service.check_user_has_role(uid, rid, indirect=True)
        assert result == {"has_role": True, "direct": False}

    def test_not_found_indirect(self, rbac_service):
        uid, rid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        query_mock = MagicMock()
        db.query.return_value = query_mock
        query_mock.filter_by.return_value.first.return_value = None
        query_mock.filter.return_value.first.return_value = None
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value=set())

        result = rbac_service.check_user_has_role(uid, rid, indirect=True)
        assert result == {"has_role": False, "direct": False}

    def test_via_group_with_ancestors(self, rbac_service):
        uid, rid, gid, aid = uuid4(), uuid4(), uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.side_effect = [None, (aid, rid)]
        db.query.return_value.filter.return_value.all.return_value = [(aid,)]
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value={gid})

        result = rbac_service.check_user_has_role(uid, rid, indirect=True)
        assert result == {"has_role": True, "direct": False}


class TestGroupHasRole:
    def test_direct(self, rbac_service):
        gid, rid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.return_value = (gid, rid)

        result = rbac_service.check_group_has_role(gid, rid)
        assert result == {"has_role": True, "direct": True}

    def test_not_direct_no_indirect(self, rbac_service):
        gid, rid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.return_value = None

        result = rbac_service.check_group_has_role(gid, rid, indirect=False)
        assert result == {"has_role": False, "direct": False}

    def test_via_ancestor(self, rbac_service):
        gid, rid, aid = uuid4(), uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.side_effect = [None, (aid, rid)]
        db.query.return_value.filter.return_value.all.return_value = [(aid,)]

        result = rbac_service.check_group_has_role(gid, rid, indirect=True)
        assert result == {"has_role": True, "direct": False}


class TestListRoleSubjects:
    def test_direct(self, rbac_service):
        rid = uuid4()
        uid1, uid2, gid = uuid4(), uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter.return_value.all.side_effect = [
            [(uid1,), (uid2,)],
            [(gid,)],
        ]

        result = rbac_service.list_role_subjects(rid, indirect=False)
        assert result["users"] == [str(uid1), str(uid2)]
        assert result["groups"] == [str(gid)]

    def test_indirect(self, rbac_service):
        rid = uuid4()
        uid, gid, member_uid = uuid4(), uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter.return_value.all.side_effect = [
            [(uid,)],
            [(gid,)],
            [(gid,)],
            [],
        ]
        rbac_service._user_groups_repo.get_user_ids_for_group = MagicMock(return_value={member_uid})

        result = rbac_service.list_role_subjects(rid, indirect=True)
        assert str(uid) in result["users"]
        assert str(gid) in result["groups"]
        assert str(member_uid) in result["indirect_users"]


class TestListRoleSubjectsDescendants:
    def test_indirect_with_descendants(self, rbac_service):
        rid = uuid4()
        uid, gid, desc_gid, member_uid = uuid4(), uuid4(), uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value

        all_results = [
            [(uid,)],
            [(gid,)],
            [(gid,)],
            [(desc_gid,)],
            [],
        ]
        db.query.return_value.filter.return_value.all.side_effect = all_results
        rbac_service._user_groups_repo.get_user_ids_for_group = MagicMock(return_value={member_uid})

        result = rbac_service.list_role_subjects(rid, indirect=True)
        assert str(uid) in result["users"]
        assert str(gid) in result["groups"]


# ==================================================================================================
# _check_policy_perm group match (line 421)
# ==================================================================================================
