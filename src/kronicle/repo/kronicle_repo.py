# kronicle/repo/kronicle_repo.py
import functools
from collections.abc import Sequence
from typing import Generic, Type, TypeVar
from uuid import UUID

from sqlalchemy.orm import Session
from sqlalchemy.sql import delete, select

from kronicle.db.base.kronicle_entity import KronicleEntity
from kronicle.utils.dev_logs import log_w


def log_repo_error(method):
    """Log the exception with the model name and re-raise."""

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except Exception as e:
            if hasattr(self, "model") and self.model is not None:
                log_w(method.__name__, self.model.__name__, type(e).__name__, str(e))
            else:
                log_w(method.__name__, type(e).__name__, str(e))
            raise

    return wrapper


T = TypeVar("T", bound=KronicleEntity)


class KronicleRepository(Generic[T]):
    """
    Translate Python calls into SQL statements.
    """

    model: Type[T]

    # ----------------------------------------------------------------------------------------------
    # Fetch methods
    # ----------------------------------------------------------------------------------------------
    @log_repo_error
    def get_by_id(self, db: Session, *, id: UUID) -> T | None:
        stmt = select(self.model).where(self.model.id == id)
        return db.execute(stmt).scalar_one_or_none()

    @log_repo_error
    def get_by_ids(self, db: Session, *, ids: set[UUID]) -> list[T]:
        if not ids:
            return []
        stmt = select(self.model).where(self.model.id.in_(ids))
        return list(db.execute(stmt).scalars().all())

    @log_repo_error
    def get_by_name(self, db: Session, *, name: str) -> T | None:
        stmt = select(self.model).where(self.model.name == name)
        return db.execute(stmt).scalar_one_or_none()

    @log_repo_error
    def fetch_all(self, db: Session) -> Sequence[T]:
        stmt = select(self.model)
        return db.execute(stmt).scalars().all()

    # ----------------------------------------------------------------------------------------------
    # Write methods
    # ----------------------------------------------------------------------------------------------
    @log_repo_error
    def add(self, db: Session, *, entity: T) -> T:
        db.add(entity)  # let DB raise if conflict
        db.flush()  # ensures id is populated
        return entity

    @log_repo_error
    def save(self, db: Session, *, entity: T) -> T:
        db.add(entity)
        db.flush()  # ensures id is populated
        return entity

    @log_repo_error
    def delete(self, db: Session, *, entity: T) -> T:
        db.delete(entity)
        db.flush()
        return entity

    @log_repo_error
    def delete_by_id(self, db: Session, *, id: UUID) -> None:
        stmt = delete(self.model).where(self.model.id == id)
        db.execute(stmt)

    @log_repo_error
    def delete_by_id_returning(self, db: Session, *, id: UUID) -> T | None:
        stmt = delete(self.model).where(self.model.id == id).returning(self.model)
        return db.execute(stmt).scalar_one_or_none()

    @log_repo_error
    def delete_by_ids(self, db: Session, *, ids: set[UUID]) -> None:
        if not ids:
            return
        stmt = delete(self.model).where(self.model.id.in_(ids))
        db.execute(stmt)

    @log_repo_error
    def delete_by_ids_returning(self, db: Session, *, ids: set[UUID]) -> list[T]:
        if not ids:
            return []
        stmt = delete(self.model).where(self.model.id.in_(ids)).returning(self.model)
        result = db.execute(stmt)
        return list(result.scalars().all())
