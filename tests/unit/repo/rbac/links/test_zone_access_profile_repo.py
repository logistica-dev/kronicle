# tests/unit/repo/rbac/links/test_zone_access_profile_repo.py
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from kronicle.db.rbac.links.rbac_access_profile import ZoneAccessProfile
from kronicle.repo.rbac.links.zone_access_profile_repo import ZoneAccessProfileRepository


@pytest.fixture
def repo():
    return ZoneAccessProfileRepository()


@pytest.fixture
def mock_db():
    return MagicMock(spec=Session)


class TestGetByRoleAndZone:
    def test_returns_profile(self, repo, mock_db):
        expected = MagicMock()
        mock_db.execute.return_value.scalar_one_or_none.return_value = expected
        result = repo.get_by_role_and_zone(mock_db, role_id=uuid4(), zone_id=uuid4())
        assert result is expected

    def test_returns_none_when_not_found(self, repo, mock_db):
        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        assert repo.get_by_role_and_zone(mock_db, role_id=uuid4(), zone_id=uuid4()) is None


class TestCreate:
    def test_adds_and_flushes_profile(self, repo, mock_db):
        result = repo.create(mock_db, role_id=uuid4(), zone_id=uuid4(), name="test_zone_profile")
        assert isinstance(result, ZoneAccessProfile)
        mock_db.add.assert_called_once()
        mock_db.flush.assert_called_once()

    def test_creates_profile_with_no_name(self, repo, mock_db):
        result = repo.create(mock_db, role_id=uuid4(), zone_id=uuid4())
        assert result is not None
        mock_db.flush.assert_called_once()
