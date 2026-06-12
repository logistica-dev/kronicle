# tests/unit/services/test_rbac_service.py
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from kronicle.db.rbac.models.rbac_user import RbacUser
from kronicle.errors.error_types import UnauthorizedError
from kronicle.schemas.permissions.permission import Permission, PermissionAction, PermissionTarget
from kronicle.schemas.rbac.safe_user_schemas import OutputUser, ProcessedUser
from kronicle.services.rbac_service import RbacService


@pytest.fixture
def mock_db_session():
    # Provide transaction and get_db mocks
    mock_session = MagicMock()
    mock_session.get_db.return_value.__enter__.return_value = MagicMock()
    mock_session.transaction.return_value.__enter__.return_value = MagicMock()
    return mock_session


@pytest.fixture
def rbac_service(mock_db_session):
    return RbacService(rbac_db_session=mock_db_session)


def test_create_user_success(rbac_service):
    email = "new@example.com"
    name = "NewUser"
    user = ProcessedUser(email=email, name=name, password_hash="hashed_pw")
    db_user = RbacUser(id=uuid4(), name=name, email=email, external_id=None, full_name=None, details={})

    rbac_service._user_repo.get_by_email = MagicMock(return_value=None)
    rbac_service._user_repo.create_user = MagicMock(return_value=db_user)

    out_user = rbac_service.create_user(user)
    assert isinstance(out_user, OutputUser)
    assert out_user.email == user.email
    assert out_user.name == user.name


def test_create_user_already_exists(rbac_service):
    user = ProcessedUser(email="existing@example.com", name="ExistingUser", password_hash="hashed_pw")
    db_user = RbacUser(
        id=uuid4(), name="ExistingUser", email="existing@example.com", external_id=None, full_name=None, details={}
    )

    rbac_service._user_repo.get_by_email = MagicMock(return_value=db_user)

    with pytest.raises(UnauthorizedError):
        rbac_service.create_user(user)


# --------------------------------------------------------------------------------------------------
# user_has_permission
# --------------------------------------------------------------------------------------------------


def test_user_has_permission_direct_role(rbac_service):
    user_id = uuid4()
    db = rbac_service._db.get_db.return_value.__enter__.return_value
    db.execute.return_value.first.return_value = (uuid4(),)

    result = rbac_service.user_has_permission(user_id, "data:read")

    assert result is True
    db.execute.assert_called_once()


def test_user_has_permission_group_role(rbac_service):
    user_id = uuid4()
    db = rbac_service._db.get_db.return_value.__enter__.return_value
    db.execute.return_value.first.side_effect = [None, (uuid4(),)]
    rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value={uuid4()})

    result = rbac_service.user_has_permission(user_id, "data:write")

    assert result is True
    assert db.execute.call_count == 2


def test_user_has_permission_no_match(rbac_service):
    user_id = uuid4()
    db = rbac_service._db.get_db.return_value.__enter__.return_value
    db.execute.return_value.first.return_value = None
    rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value=set())

    result = rbac_service.user_has_permission(user_id, "data:read")

    assert result is False
    db.execute.assert_called_once()


def test_user_has_permission_empty_groups_no_match(rbac_service):
    user_id = uuid4()
    db = rbac_service._db.get_db.return_value.__enter__.return_value
    db.execute.return_value.first.side_effect = [None, None]
    rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value={uuid4()})

    result = rbac_service.user_has_permission(user_id, "data:read")

    assert result is False


def test_user_has_permission_with_permission_object(rbac_service):
    user_id = uuid4()
    db = rbac_service._db.get_db.return_value.__enter__.return_value
    db.execute.return_value.first.return_value = (uuid4(),)

    perm = Permission(PermissionTarget.DATA, PermissionAction.READ)
    result = rbac_service.user_has_permission(user_id, perm)

    assert result is True
    db.execute.assert_called_once()
