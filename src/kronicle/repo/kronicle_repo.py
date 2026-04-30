# kronicle/repo/kronicle_repo.py
from collections.abc import Sequence
from typing import Generic, Type, TypeVar
from uuid import UUID

from sqlalchemy.orm import Session
from sqlalchemy.sql import delete, select

from kronicle.db.base.kronicle_entity import KronicleEntity

T = TypeVar("T", bound=KronicleEntity)


class KronicleRepository(Generic[T]):
    """
    Translate Python calls into SQL statements.
    """

    model: Type[T]

    # ----------------------------------------------------------------------------------------------
    # Fetch methods
    # ----------------------------------------------------------------------------------------------
    def get_by_id(self, db: Session, *, id: UUID) -> T | None:
        stmt = select(self.model).where(self.model.id == id)
        return db.execute(stmt).scalar_one_or_none()

    def get_by_ids(self, db: Session, *, ids: set[UUID]) -> list[T]:
        if not ids:
            return []
        stmt = select(self.model).where(self.model.id.in_(ids))
        return list(db.execute(stmt).scalars().all())

    def get_by_name(self, db: Session, *, name: str) -> T | None:
        stmt = select(self.model).where(self.model.name == name)
        return db.execute(stmt).scalar_one_or_none()

    def fetch_all(self, db: Session) -> Sequence[T]:
        stmt = select(self.model)
        return db.execute(stmt).scalars().all()

    # ----------------------------------------------------------------------------------------------
    # Write methods
    # ----------------------------------------------------------------------------------------------
    def add(self, db: Session, *, entity: T) -> T:
        db.add(entity)  # let DB raise if conflict
        db.flush()  # ensures id is populated
        return entity

    def save(self, db: Session, *, entity: T) -> T:
        db.add(entity)
        db.flush()  # ensures id is populated
        return entity

    def delete(self, db: Session, *, entity: T) -> T:
        db.delete(entity)
        db.flush()
        return entity

    def delete_by_id(self, db: Session, *, id: UUID) -> None:
        stmt = delete(self.model).where(self.model.id == id).returning(self.model)
        db.execute(stmt)

    def delete_by_id_returning(self, db: Session, *, id: UUID) -> T | None:
        stmt = delete(self.model).where(self.model.id == id).returning(self.model)
        return db.execute(stmt).scalar_one_or_none()

    def delete_by_ids(self, db: Session, *, ids: set[UUID]) -> None:
        if not ids:
            return
        stmt = delete(self.model).where(self.model.id.in_(ids))
        db.execute(stmt)

    def delete_by_ids_returning(self, db: Session, *, ids: set[UUID]) -> list[T]:
        if not ids:
            return []
        stmt = delete(self.model).where(self.model.id.in_(ids)).returning(self.model)
        result = db.execute(stmt)
        return list(result.scalars().all())
