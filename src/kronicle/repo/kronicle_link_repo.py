# kronicle/db/base/kronicle_repo.py
from typing import Generic, Type, TypeVar

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from sqlalchemy.sql import delete

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
    def remove_link(cls, db: Session, filters):
        stmt = delete(cls.model).where(*[col == val for col, val in filters.items()])
        db.execute(stmt)

    def add_parent(self, db: Session, parent: T, child: T):
        self.ensure_link(db, {self.model.PARENT_ID: parent.id, self.model.CHILD_ID: child.id})

    def remove_parent(self, db: Session, parent: T, child: T):
        self.remove_link(db, {self.model.PARENT_ID: parent.id, self.model.CHILD_ID: child.id})
