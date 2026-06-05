# kronicle/repo/hierarchy/hierarchy_service.py
from typing import Callable, Generic, TypeVar

from sqlalchemy.orm import Session

from kronicle.db.base.kronicle_base import KronicleBase
from kronicle.repo.hierarchy.hierarchy_engine import HierarchyEngine

T = TypeVar("T", bound=KronicleBase)


class HierarchyService(Generic[T]):
    """
    High-level API for hierarchy operations.

    This is the only class that application code should use directly.
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

    def descendant_closure(self, nodes: list[T]) -> set[T]:
        """
        Expand multiple roots into full closure set.
        Used heavily by RBAC resolution.
        """
        result = {}
        for node in nodes:
            for d in self.engine.descendants(node):
                result[d.id] = d
            result[node.id] = node
        return set(result.values())
