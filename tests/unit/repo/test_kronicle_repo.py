from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from kronicle.db.base.kronicle_entity import KronicleEntity
from kronicle.repo.kronicle_repo import KronicleRepository


class TestEntity(KronicleEntity):
    __tablename__ = "test_repo_entities"
    __table_args__ = {"extend_existing": True}

    @classmethod
    def namespace(cls):
        return "test_schema"


class TestRepo(KronicleRepository[TestEntity]):
    model = TestEntity


@pytest.fixture
def repo():
    return TestRepo()


@pytest.fixture
def mock_db():
    return MagicMock(spec=Session)


def make_entity(id=None):
    e = TestEntity()
    e.id = id or uuid4()
    e.name = "test"
    return e


class TestGetById:
    def test_returns_entity_when_found(self, repo, mock_db):
        entity = make_entity()
        mock_db.execute.return_value.scalar_one_or_none.return_value = entity

        result = repo.get_by_id(mock_db, id=entity.id)

        assert result is entity
        mock_db.execute.assert_called_once()

    def test_returns_none_when_not_found(self, repo, mock_db):
        mock_db.execute.return_value.scalar_one_or_none.return_value = None

        result = repo.get_by_id(mock_db, id=uuid4())

        assert result is None


class TestGetByIds:
    def test_returns_empty_list_for_empty_set(self, repo, mock_db):
        result = repo.get_by_ids(mock_db, ids=set())
        assert result == []
        mock_db.execute.assert_not_called()

    def test_returns_matching_entities(self, repo, mock_db):
        ids = {uuid4(), uuid4()}
        mock_db.execute.return_value.scalars.return_value.all.return_value = [make_entity(), make_entity()]

        result = repo.get_by_ids(mock_db, ids=ids)

        assert len(result) == 2
        mock_db.execute.assert_called_once()


class TestGetByName:
    def test_returns_entity_when_found(self, repo, mock_db):
        entity = make_entity()
        mock_db.execute.return_value.scalar_one_or_none.return_value = entity

        result = repo.get_by_name(mock_db, name="test")

        assert result is entity

    def test_returns_none_when_not_found(self, repo, mock_db):
        mock_db.execute.return_value.scalar_one_or_none.return_value = None

        result = repo.get_by_name(mock_db, name="missing")

        assert result is None


class TestFetchAll:
    def test_returns_all_entities(self, repo, mock_db):
        entities = [make_entity(), make_entity()]
        mock_db.execute.return_value.scalars.return_value.all.return_value = entities

        result = repo.fetch_all(mock_db)

        assert result == entities
        mock_db.execute.assert_called_once()


class TestAdd:
    def test_adds_and_flushes(self, repo, mock_db):
        entity = make_entity()

        result = repo.add(mock_db, entity=entity)

        assert result is entity
        mock_db.add.assert_called_once_with(entity)
        mock_db.flush.assert_called_once()


class TestSave:
    def test_saves_and_flushes(self, repo, mock_db):
        entity = make_entity()

        result = repo.save(mock_db, entity=entity)

        assert result is entity
        mock_db.add.assert_called_once_with(entity)
        mock_db.flush.assert_called_once()


class TestDelete:
    def test_deletes_and_flushes(self, repo, mock_db):
        entity = make_entity()

        result = repo.delete(mock_db, entity=entity)

        assert result is entity
        mock_db.delete.assert_called_once_with(entity)
        mock_db.flush.assert_called_once()


class TestDeleteById:
    def test_executes_delete_statement(self, repo, mock_db):
        entity_id = uuid4()

        repo.delete_by_id(mock_db, id=entity_id)

        mock_db.execute.assert_called_once()


class TestDeleteByIdReturning:
    def test_returns_deleted_entity(self, repo, mock_db):
        entity = make_entity()
        mock_db.execute.return_value.scalar_one_or_none.return_value = entity

        result = repo.delete_by_id_returning(mock_db, id=entity.id)

        assert result is entity
        mock_db.execute.assert_called_once()

    def test_returns_none_when_not_found(self, repo, mock_db):
        mock_db.execute.return_value.scalar_one_or_none.return_value = None

        result = repo.delete_by_id_returning(mock_db, id=uuid4())

        assert result is None


class TestDeleteByIds:
    def test_skips_empty_set(self, repo, mock_db):
        repo.delete_by_ids(mock_db, ids=set())
        mock_db.execute.assert_not_called()

    def test_executes_delete_for_ids(self, repo, mock_db):
        repo.delete_by_ids(mock_db, ids={uuid4(), uuid4()})
        mock_db.execute.assert_called_once()


class TestDeleteByIdsReturning:
    def test_skips_empty_set(self, repo, mock_db):
        result = repo.delete_by_ids_returning(mock_db, ids=set())
        assert result == []
        mock_db.execute.assert_not_called()

    def test_returns_deleted_entities(self, repo, mock_db):
        mock_db.execute.return_value.scalars.return_value.all.return_value = [make_entity(), make_entity()]

        result = repo.delete_by_ids_returning(mock_db, ids={uuid4(), uuid4()})

        assert len(result) == 2
        mock_db.execute.assert_called_once()


class TestLogRepoError:
    def test_logs_and_reraises(self, mock_db):
        repo = TestRepo()
        mock_db.execute.side_effect = ValueError("DB error")

        with patch("kronicle.repo.kronicle_repo.log_w") as mock_log_w:
            with pytest.raises(ValueError, match="DB error"):
                repo.fetch_all(mock_db)

        mock_log_w.assert_called_once()
