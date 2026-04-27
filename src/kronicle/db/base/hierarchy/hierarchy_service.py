from typing import Callable, Generic, TypeVar

from sqlalchemy.orm import Session

from kronicle.db.base.hierarchy.hierarchy_engine import HierarchyEngine
from kronicle.db.base.kronicle_base import KronicleBase

T = TypeVar("T", bound=KronicleBase)


class HierarchyService(Generic[T]):
    """
    High-level API for hierarchy operations.

    This is the only class that application code should use directly.

    It composes:
      - HierarchyEngine (graph traversal)
      - HierarchyRepository (data access)
      - HierarchyValidator (constraints)
      => Repository writes, Validator decides, Service orchestrates
    """

    def __init__(
        self,
        engine: HierarchyEngine[T],
        add_edge: Callable[[Session, T, T]],
        remove_edge: Callable[[Session, T, T]],
        max_parents: int = 1,
    ):
        self.engine = engine
        self.add_edge = add_edge
        self.remove_edge = remove_edge
        self.max_parents = max_parents

    def add_parent(self, session: Session, parent: T, child: T) -> None:
        """
        Safely add parent -> child relationship.
        """
        # --- cycle protection ---
        if self.engine.would_create_cycle(parent, child):
            raise ValueError("Cycle detected")

        # --- max parents constraint ---
        if self.max_parents is not None:
            parents = list(self.engine.parents_of(child))
            if len(parents) >= self.max_parents:
                raise ValueError("Max parents exceeded")

        self.add_edge(session, parent, child)

    def remove_parent(self, session: Session, parent: T, child: T) -> None:
        self.remove_edge(session, parent, child)

    def ancestors(self, node):
        """
        Return all ancestors of a node.
        """
        return list(self.engine.ancestors(node))

    def descendants(self, node):
        """
        Return all descendants of a node.
        """
        return list(self.engine.descendants(node))
