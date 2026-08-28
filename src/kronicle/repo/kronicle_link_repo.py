# kronicle/repo/kronicle_link_repo.py
from typing import Generic, Type, TypeVar

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from sqlalchemy.sql import delete, select

from kronicle.db.base.kronicle_link import KronicleLink

T = TypeVar("T", bound=KronicleLink)


class KronicleLinkRepository(Generic[T]):
    """
    Translate Python calls into SQL statements.
    Mechanical SQL patterns, not relationship semantics!
    """

    model: Type[T]

    @classmethod
    def ensure_link(cls, db: Session, values: dict):
        """
        duplicate is ignored (idempotent operation)
        """
        stmt = insert(cls.model).values(**values).on_conflict_do_nothing(constraint=cls.model.uq_constraint())
        db.execute(stmt)

    @classmethod
    def check_link(cls, db: Session, filters) -> T | None:
        stmt = select(cls.model).where(*[col == val for col, val in filters.items()])
        return db.execute(stmt).scalars().first()

    @classmethod
    def list_links(cls, db: Session, filters) -> list[T]:
        stmt = select(cls.model).where(*[col == val for col, val in filters.items()])
        return list(db.execute(stmt).scalars().all())

    @classmethod
    def remove_link(cls, db: Session, filters):
        stmt = delete(cls.model).where(*[col == val for col, val in filters.items()])
        db.execute(stmt)

    @classmethod
    def ensure_link_returning(cls, db: Session, values: dict) -> T:
        """Upsert (idempotent) and return the row — new or existing.
        Uses a no-op UPDATE on conflict so RETURNING always yields the row."""
        stmt = (
            insert(cls.model)
            .values(**values)
            .on_conflict_do_update(
                constraint=cls.model.uq_constraint(),
                set_=dict(values),  # no-op: sets each column to its own value
            )
            .returning(cls.model)
        )
        return db.execute(stmt).scalars().one()

    @classmethod
    def remove_link_returning(cls, db: Session, filters) -> T | None:
        """Delete and return the removed row, or None if it didn't exist."""
        stmt = delete(cls.model).where(*[col == val for col, val in filters.items()]).returning(cls.model)
        return db.execute(stmt).scalars().first()
