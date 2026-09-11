# tests/unit/repo/test_kronicle_link_repo.py
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from kronicle.db.rbac.links.user_roles import RbacUserRoles
from kronicle.repo.kronicle_link_repo import KronicleLinkRepository


class TestRepo(KronicleLinkRepository[RbacUserRoles]):
    model = RbacUserRoles


@pytest.fixture
def repo():
    return TestRepo()


@pytest.fixture
def mock_db():
    return MagicMock(spec=Session)


def test_cols_resolves_filters_to_column_expressions(repo):
    user_id, role_id = uuid4(), uuid4()
    cols = repo._cols({"user_id": user_id, "role_id": role_id})
    assert len(cols) == 2


class TestEnsureLink:
    def test_executes_upsert(self, repo, mock_db):
        user_id, role_id = uuid4(), uuid4()
        repo.ensure_link(mock_db, {"user_id": user_id, "role_id": role_id})
        assert mock_db.execute.call_count == 1


class TestCheckLink:
    def test_returns_first_row(self, repo, mock_db):
        expected = MagicMock()
        mock_db.execute.return_value.scalars.return_value.first.return_value = expected
        assert repo.check_link(mock_db, {"user_id": uuid4()}) is expected


class TestListLinks:
    def test_returns_all_rows(self, repo, mock_db):
        expected = [MagicMock(), MagicMock()]
        mock_db.execute.return_value.scalars.return_value.all.return_value = expected
        assert repo.list_links(mock_db, {"user_id": uuid4()}) == expected


class TestRemoveLink:
    def test_executes_delete(self, repo, mock_db):
        repo.remove_link(mock_db, {"user_id": uuid4()})
        assert mock_db.execute.call_count == 1


class TestEnsureLinkReturning:
    def test_returns_inserted_row(self, repo, mock_db):
        expected = MagicMock()
        mock_db.execute.return_value.scalars.return_value.one.return_value = expected
        assert repo.ensure_link_returning(mock_db, {"user_id": uuid4(), "role_id": uuid4()}) is expected


class TestRemoveLinkReturning:
    def test_returns_deleted_row(self, repo, mock_db):
        expected = MagicMock()
        mock_db.execute.return_value.scalars.return_value.first.return_value = expected
        assert repo.remove_link_returning(mock_db, {"user_id": uuid4()}) is expected
