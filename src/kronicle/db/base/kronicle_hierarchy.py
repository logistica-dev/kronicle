# kronicle/db/base/kronicle_hierarchy.py
from __future__ import annotations

from typing import ClassVar, Generic, Type, TypeVar
from uuid import UUID

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, declared_attr, mapped_column, relationship

from kronicle.db.base.kronicle_base import KronicleBase
from kronicle.db.base.kronicle_link import KronicleLink

N = TypeVar("N", bound=KronicleBase)


def _node_fk_target(cls) -> str:
    node = cls.node_model
    return f"{node.namespace()}.{node.__tablename__}.id"  # "core.zones.id" / "rbac.groups.id"


class KronicleHierarchy(KronicleLink, Generic[N]):
    __abstract__ = True  # Do not create a table for this class itself

    node_model: Type[N]

    PARENT_ID: ClassVar[str] = "parent_id"
    CHILD_ID: ClassVar[str] = "child_id"

    PARENT: ClassVar[str] = "parent"
    CHILD: ClassVar[str] = "child"

    PARENTS: ClassVar[str] = "parent_links"
    CHILDREN: ClassVar[str] = "child_links"

    # Note: ondelete=CASCADE
    # => If a referenced row is deleted, automatically delete the rows in this table that point to it.
    @declared_attr
    def parent_id(cls) -> Mapped[UUID]:
        return mapped_column(ForeignKey(_node_fk_target(cls), ondelete="CASCADE"), primary_key=True)

    @declared_attr
    def child_id(cls) -> Mapped[UUID]:
        return mapped_column(ForeignKey(_node_fk_target(cls), ondelete="CASCADE"), primary_key=True)

    @declared_attr
    def parent(cls) -> Mapped[N | None]:
        return relationship(
            cls.node_model,
            foreign_keys=[cls.parent_id],  # pyright: ignore[reportArgumentType]
            passive_deletes=True,
        )

    @declared_attr
    def child(cls) -> Mapped[N | None]:
        return relationship(
            cls.node_model,
            foreign_keys=[cls.child_id],  # pyright: ignore[reportArgumentType]
            passive_deletes=True,
        )
