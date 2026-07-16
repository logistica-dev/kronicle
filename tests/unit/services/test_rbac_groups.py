# tests/unit/services/test_rbac_groups.py
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from kronicle.errors.error_types import BadRequestError, ConflictError, NotFoundError
from kronicle.schemas.rbac.safe_group_schemas import OutputGroup
from tests.unit.services.conftest import fake_group, fake_user


class TestGroups:
    def test_get_user_groups(self, rbac_service):
        gid = uuid4()
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value={gid})
        result = rbac_service.get_user_groups(uuid4())
        assert result == [gid]

    @patch("kronicle.services.rbac_service.RbacGroup")
    def test_create_group(self, mock_rbac_group, rbac_service):
        mock_group = MagicMock()
        mock_group.id = uuid4()
        mock_group.name = "test-group"
        mock_group.details = {"k": "v"}
        mock_rbac_group.return_value = mock_group
        rbac_service._user_repo.get_by_name = MagicMock(return_value=None)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        out = rbac_service.create_group("test-group", details={"k": "v"})
        assert isinstance(out, OutputGroup)
        assert out.name == "test-group"

    def test_create_group_duplicate(self, rbac_service):
        rbac_service._user_repo.get_by_name = MagicMock(return_value=None)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=fake_group(name="dup"))
        with pytest.raises(BadRequestError, match="already exists"):
            rbac_service.create_group("dup")

    def test_get_groups(self, rbac_service):
        rbac_service._group_repo.fetch_all = MagicMock(return_value=[fake_group(), fake_group()])
        result = rbac_service.get_groups()
        assert len(result) == 2

    def test_get_group_by_id(self, rbac_service):
        gid = uuid4()
        rbac_service._group_repo.get_by_id = MagicMock(return_value=fake_group(id=gid))
        result = rbac_service.get_group_by_id(gid)
        assert isinstance(result, OutputGroup)
        assert result.id == gid

    def test_get_group_by_id_none(self, rbac_service):
        rbac_service._group_repo.get_by_id = MagicMock(return_value=None)
        assert rbac_service.get_group_by_id(uuid4()) is None

    def test_get_group_by_name(self, rbac_service):
        rbac_service._group_repo.get_by_name = MagicMock(return_value=fake_group(name="found"))
        result = rbac_service.get_group_by_name("found")
        assert isinstance(result, OutputGroup)
        assert result.name == "found"

    def test_get_group_by_name_none(self, rbac_service):
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        assert rbac_service.get_group_by_name("noone") is None

    def test_get_users_from_group(self, rbac_service):
        uid = uuid4()
        rbac_service._user_groups_repo.get_user_ids_for_group = MagicMock(return_value={uid})
        rbac_service._user_repo.get_by_id = MagicMock(return_value=fake_user(id=uid))
        result = rbac_service.get_users_from_group(group_id=uuid4())
        assert len(result) == 1
        assert result[0].id == uid

    def test_get_users_from_group_skips_missing(self, rbac_service):
        rbac_service._user_groups_repo.get_user_ids_for_group = MagicMock(return_value={uuid4()})
        rbac_service._user_repo.get_by_id = MagicMock(return_value=None)
        assert rbac_service.get_users_from_group(group_id=uuid4()) == []

    def test_patch_group(self, rbac_service):
        gid = uuid4()
        grp = fake_group(id=gid, name="old")
        rbac_service._group_repo.get_by_id = MagicMock(return_value=grp)

        out = rbac_service.patch_group(gid, name="new", details={"k": "v"})
        assert out.name == "new"
        assert grp.name == "new"
        assert grp.details == {"k": "v"}

    def test_patch_group_not_found(self, rbac_service):
        rbac_service._group_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.patch_group(uuid4(), name="x")

    def test_delete_group_force(self, rbac_service):
        gid = uuid4()
        grp = fake_group(id=gid, name="del")
        rbac_service._group_repo.get_by_id = MagicMock(return_value=grp)
        out = rbac_service.delete_group(gid, force=True)
        assert isinstance(out, OutputGroup)

    def test_delete_group_not_found(self, rbac_service):
        rbac_service._group_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.delete_group(uuid4())

    def test_delete_group_with_users(self, rbac_service):
        gid = uuid4()
        grp = fake_group(id=gid, name="del")
        rbac_service._group_repo.get_by_id = MagicMock(return_value=grp)
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        db.execute.return_value.scalars.return_value.all.return_value = [fake_user()]

        with pytest.raises(ConflictError, match="cannot be deleted"):
            rbac_service.delete_group(gid, force=False)

    def test_add_user_to_group(self, rbac_service):
        uid, gid = uuid4(), uuid4()
        rbac_service._user_repo.get_by_id = MagicMock(return_value=fake_user(id=uid))
        rbac_service._group_repo.get_by_id = MagicMock(return_value=fake_group(id=gid))
        rbac_service._user_groups_repo.add_user_to_group = MagicMock()

        rbac_service.add_user_to_group(uid, gid)
        rbac_service._user_groups_repo.add_user_to_group.assert_called_once()

    def test_add_user_to_group_user_not_found(self, rbac_service):
        rbac_service._user_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="User"):
            rbac_service.add_user_to_group(uuid4(), uuid4())

    def test_add_user_to_group_group_not_found(self, rbac_service):
        rbac_service._user_repo.get_by_id = MagicMock(return_value=fake_user())
        rbac_service._group_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Group"):
            rbac_service.add_user_to_group(uuid4(), uuid4())

    def test_remove_user_from_group(self, rbac_service):
        uid, gid = uuid4(), uuid4()
        rbac_service._user_repo.get_by_id = MagicMock(return_value=fake_user(id=uid))
        rbac_service._group_repo.get_by_id = MagicMock(return_value=fake_group(id=gid))
        rbac_service._user_groups_repo.remove_user_from_group = MagicMock()

        rbac_service.remove_user_from_group(uid, gid)
        rbac_service._user_groups_repo.remove_user_from_group.assert_called_once()

    def test_remove_user_from_group_user_not_found(self, rbac_service):
        rbac_service._user_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="User"):
            rbac_service.remove_user_from_group(uuid4(), uuid4())


# ==================================================================================================
# Role methods
# ==================================================================================================


class TestUserInGroup:
    def test_direct(self, rbac_service):
        uid, gid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.return_value = (uid, gid)

        result = rbac_service.check_user_in_group(uid, gid)
        assert result == {"is_member": True, "direct": True}

    def test_not_direct_no_indirect(self, rbac_service):
        uid, gid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.return_value = None

        result = rbac_service.check_user_in_group(uid, gid, indirect=False)
        assert result == {"is_member": False, "direct": False}

    def test_via_descendant(self, rbac_service):
        uid, gid, did = uuid4(), uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.return_value = None
        db.query.return_value.filter.return_value.all.return_value = [(did,)]
        rbac_service._user_groups_repo.get_user_ids_for_group = MagicMock(return_value={uid})

        result = rbac_service.check_user_in_group(uid, gid, indirect=True)
        assert result == {"is_member": True, "direct": False}

    def test_not_found_indirect(self, rbac_service):
        uid, gid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.return_value = None
        db.query.return_value.filter.return_value.all.return_value = []

        result = rbac_service.check_user_in_group(uid, gid, indirect=True)
        assert result == {"is_member": False, "direct": False}

    def test_via_descendant_no_membership(self, rbac_service):
        uid, gid, did = uuid4(), uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.return_value = None
        db.query.return_value.filter.return_value.all.return_value = [(did,)]
        rbac_service._user_groups_repo.get_user_ids_for_group = MagicMock(return_value=set())

        result = rbac_service.check_user_in_group(uid, gid, indirect=True)
        assert result == {"is_member": False, "direct": False}


# ==================================================================================================
# Hierarchy helpers
# ==================================================================================================


class TestRemoveUserFromGroupEdges:
    def test_remove_user_from_group_group_not_found(self, rbac_service):
        uid = uuid4()
        rbac_service._user_repo.get_by_id = MagicMock(return_value=fake_user(id=uid))
        rbac_service._group_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Group"):
            rbac_service.remove_user_from_group(uid, uuid4())


# ==================================================================================================
# list_role_subjects with descendants
# ==================================================================================================
