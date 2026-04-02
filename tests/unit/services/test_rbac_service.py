# tests/unit/services/test_rbac_service.py
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from kronicle.db.rbac.models.rbac_user import RbacUser
from kronicle.errors.error_types import UnauthorizedError
from kronicle.schemas.rbac.safe_user_schemas import OutputUser, ProcessedUser
from kronicle.services.rbac_service import RbacService


@pytest.fixture
def mock_engine():
    return MagicMock()


@pytest.fixture
def mock_db_session():
    # Provide transaction and get_db mocks
    mock_session = MagicMock()
    mock_session.get_db.return_value.__enter__.return_value = MagicMock()
    mock_session.transaction.return_value.__enter__.return_value = MagicMock()
    return mock_session


@pytest.fixture
def rbac_service(mock_db_session, mock_engine):
    return RbacService(rbac_db_session=mock_db_session, rbac_engine=mock_engine)


def test_create_user_success(rbac_service, mock_engine, mock_db_session):
    user = ProcessedUser(email="new@example.com", name="NewUser", password_hash="hashed_pw")
    db_user = RbacUser(
        id=uuid4(), name="NewUser", email="new@example.com", external_id=None, full_name=None, details={}
    )
    mock_engine.fetch_user_by_email.return_value = None
    mock_engine.create_user.return_value = db_user

    out_user = rbac_service.create_user(user)
    assert isinstance(out_user, OutputUser)
    assert out_user.email == user.email
    assert out_user.name == user.name


def test_create_user_already_exists(rbac_service, mock_engine):
    user = ProcessedUser(email="existing@example.com", name="ExistingUser", password_hash="hashed_pw")
    db_user = RbacUser(
        id=uuid4(), name="ExistingUser", email="existing@example.com", external_id=None, full_name=None, details={}
    )
    mock_engine.fetch_user_by_email.return_value = db_user

    with pytest.raises(UnauthorizedError):
        rbac_service.create_user(user)
