from datetime import datetime
from uuid import uuid4

from kronicle.db.base.kronicle_entity import KronicleEntity


class ConcreteEntity(KronicleEntity):
    __tablename__ = "test_entities"

    @classmethod
    def namespace(cls):
        return "test_schema"


def test_row_snapshot_includes_all_fields():
    entity = ConcreteEntity()
    entity.id = uuid4()
    entity.name = "test-name"
    entity.created_at = datetime(2024, 1, 1)
    entity.updated_at = datetime(2024, 1, 2)
    entity.details = {"key": "val"}

    snap = entity.row_snapshot

    assert snap["id"] == entity.id.hex
    assert snap["name"] == "test-name"
    assert snap["created_at"] == "2024-01-01T00:00:00"
    assert snap["updated_at"] == "2024-01-02T00:00:00"
    assert snap["details"] == {"key": "val"}


def test_row_snapshot_handles_none_dates():
    entity = ConcreteEntity()
    entity.id = uuid4()
    entity.name = None  # type: ignore[assignment]
    entity.created_at = None  # type: ignore[assignment]
    entity.updated_at = None  # type: ignore[assignment]
    entity.details = {}

    snap = entity.row_snapshot

    assert snap["name"] is None
    assert snap["created_at"] is None
    assert snap["updated_at"] is None
    assert snap["details"] == {}


def test_str_representation():
    entity = ConcreteEntity()
    entity.id = uuid4()
    entity.name = "my-entity"
    entity.details = {"foo": "bar"}

    result = str(entity)
    assert "ConcreteEntity" in result
    assert str(entity.id) in result
    assert "my-entity" in result
