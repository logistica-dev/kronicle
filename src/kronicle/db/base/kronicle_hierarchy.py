# kronicle/db/base/kronicle_hierarchy.py
from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar
from uuid import UUID

from sqlalchemy import Column, ForeignKey, Table
from sqlalchemy.orm import relationship

from kronicle.db.base.kronicle_base import KronicleBase
from kronicle.utils.dev_logs import log_d

T = TypeVar("T", bound="KronicleHierarchyMixin")


class KronicleHierarchyMixin(KronicleBase):
    """
    Generic hierarchical mixin:
      - Creates canonical parent->child table per subclass
      - Adds `children` relationship dynamically
      - Provides traversal, cycle detection, descendant collection
      - Provides automatic `add_child` / `remove_child`
    """

    __abstract__ = True  # no table for the base class

    _hierarchy_table: Table  # dynamically created per subclass

    # ----------------------------
    # Setup
    # ----------------------------
    @classmethod
    def _setup_hierarchy(cls):
        """Create canonical hierarchy table and attach children relationship."""
        parent_name = cls.tablename()
        namespace = cls.namespace()
        table_name = f"{parent_name}_hierarchy"

        here = f"{namespace}.{parent_name}._setup_hierarchy"
        if hasattr(cls, "_hierarchy_table"):
            log_d(here, f"Hierarchy already set, {table_name} exists")
            return
        parent_table = cls.__table__
        metadata = parent_table.metadata

        log_d(here, "table_name:", table_name)
        hierarchy_table = Table(
            table_name,
            metadata,
            Column("parent_id", parent_table.c.id.type, ForeignKey(parent_table.c.id), primary_key=True),
            Column("child_id", parent_table.c.id.type, ForeignKey(parent_table.c.id), primary_key=True),
            schema=namespace,
        )
        cls._hierarchy_table = hierarchy_table
        # Add children relationship dynamically
        cls._children = relationship(
            cls.__name__,
            secondary=hierarchy_table,
            primaryjoin=lambda: cls.id == hierarchy_table.c.parent_id,
            secondaryjoin=lambda: cls.id == hierarchy_table.c.child_id,
            backref=f"parent_{parent_name}",
        )

    # ----------------------------
    # Traversal
    # ----------------------------
    def _walk_subtree(self: T, visitor: Callable[[T], None]) -> None:
        visited: set[UUID] = set()
        stack: list[T] = [self]
        while stack:
            node = stack.pop()
            if node.id in visited:
                continue
            visited.add(node.id)
            visitor(node)
            stack.extend(node.children)

    def _walk_subtree_until(self: T, predicate: Callable[[T], bool]) -> bool:
        visited: set[UUID] = set()
        stack: list[T] = [self]
        while stack:
            node = stack.pop()
            if node.id in visited:
                continue
            visited.add(node.id)
            if predicate(node):
                return True
            stack.extend(node.children)
        return False

    @property
    def children(self: T) -> list[T]:
        """Typed access to children, backed by dynamic SQLAlchemy relationship."""
        return getattr(self, "_children", [])

    @property
    def descendants(self) -> set[UUID]:
        result: set[UUID] = set()
        self._walk_subtree(lambda node: result.add(node.id) if node.id != self.id else None)
        return result

    def _would_create_cycle(self: T, candidate: T) -> bool:
        return candidate._walk_subtree_until(lambda node: node.id == self.id)

    # ----------------------------
    # Automatic mutation
    # ----------------------------
    def add_child(self: T, child: T):
        """Add a child safely; raises ValueError on cycle."""
        if self._would_create_cycle(child):
            raise ValueError(f"Cannot add {child} as child of {self}: cycle detected")
        if child not in self.children:
            self.children.append(child)

    def remove_child(self: T, child: T):
        """Remove a child if present."""
        if child in self.children:
            self.children.remove(child)
