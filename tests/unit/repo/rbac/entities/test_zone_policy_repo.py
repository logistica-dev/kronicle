# tests/unit/repo/rbac/entities/test_zone_policy_repo.py
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from kronicle.repo.rbac.entities.zone_policy_repo import ZonePolicyRepository


@pytest.fixture
def repo():
    return ZonePolicyRepository()


@pytest.fixture
def mock_db():
    return MagicMock(spec=Session)


class TestGetBySubjectAndAccessProfile:
    def test_returns_policy(self, repo, mock_db):
        expected = MagicMock()
        mock_db.execute.return_value.scalar_one_or_none.return_value = expected
        result = repo.get_by_subject_and_access_profile(mock_db, subject_id=uuid4(), access_profile_id=uuid4())
        assert result is expected

    def test_returns_none_when_not_found(self, repo, mock_db):
        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        assert repo.get_by_subject_and_access_profile(mock_db, subject_id=uuid4(), access_profile_id=uuid4()) is None


class TestGetPoliciesForZone:
    def test_returns_policies(self, repo, mock_db):
        policies = [MagicMock()]
        mock_db.execute.return_value.scalars.return_value.all.return_value = policies
        result = repo.get_policies_for_zone(mock_db, zone_id=uuid4())
        assert result == policies

    def test_returns_empty_list(self, repo, mock_db):
        mock_db.execute.return_value.scalars.return_value.all.return_value = []
        assert repo.get_policies_for_zone(mock_db, zone_id=uuid4()) == []


class TestGetPoliciesForSubjects:
    def test_returns_policies(self, repo, mock_db):
        policies = [MagicMock()]
        mock_db.execute.return_value.scalars.return_value.all.return_value = policies
        result = repo.get_policies_for_subjects(mock_db, subject_ids=[uuid4()])
        assert result == policies

    def test_empty_subject_ids_returns_empty(self, repo, mock_db):
        assert repo.get_policies_for_subjects(mock_db, subject_ids=[]) == []
        mock_db.execute.assert_not_called()
