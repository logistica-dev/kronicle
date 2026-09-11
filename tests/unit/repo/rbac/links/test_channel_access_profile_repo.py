# tests/unit/repo/rbac/links/test_channel_access_profile_repo.py
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from kronicle.db.rbac.links.rbac_access_profile import ChannelAccessProfile
from kronicle.repo.rbac.links.channel_access_profile_repo import ChannelAccessProfileRepository


@pytest.fixture
def repo():
    return ChannelAccessProfileRepository()


@pytest.fixture
def mock_db():
    return MagicMock(spec=Session)


class TestGetByRoleAndChannel:
    def test_returns_profile(self, repo, mock_db):
        expected = MagicMock()
        mock_db.execute.return_value.scalar_one_or_none.return_value = expected
        result = repo.get_by_role_and_channel(mock_db, role_id=uuid4(), channel_id=uuid4())
        assert result is expected

    def test_returns_none_when_not_found(self, repo, mock_db):
        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        assert repo.get_by_role_and_channel(mock_db, role_id=uuid4(), channel_id=uuid4()) is None


class TestCreate:
    def test_adds_and_flushes_profile(self, repo, mock_db):
        result = repo.create(mock_db, role_id=uuid4(), channel_id=uuid4(), name="test_profile")
        assert isinstance(result, ChannelAccessProfile)
        mock_db.add.assert_called_once()
        mock_db.flush.assert_called_once()

    def test_creates_profile_with_no_name(self, repo, mock_db):
        result = repo.create(mock_db, role_id=uuid4(), channel_id=uuid4())
        assert result is not None
        mock_db.flush.assert_called_once()
