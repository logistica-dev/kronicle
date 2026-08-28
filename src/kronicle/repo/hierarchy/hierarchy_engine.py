# kronicle/repo/hierarchy/hierarchy_engine.py
from collections.abc import Callable, Iterable, Iterator
from typing import Generic, TypeVar
from uuid import UUID

from kronicle.db.base.kronicle_base import KronicleBase

T = TypeVar("T", bound=KronicleBase)


class HierarchyEngine(Generic[T]):
    """
    Generic DAG traversal engine.

    Operates on a node type T using two adjacency functions:
      - parents_of(node) -> iterable[T]
      - children_of(node) -> iterable[T]

    These functions are typically backed by SQLAlchemy relationships,
    but this class is completely ORM-agnostic.
    """

    def __init__(
        self,
        *,
        parents_of: Callable[[T], Iterable[T]],
        children_of: Callable[[T], Iterable[T]],
    ):
        self.parents_of = parents_of
        self.children_of = children_of

    # ----------------------------------------------------------------------------------------------
    # Ancestors
    # ----------------------------------------------------------------------------------------------
    def ancestors(self, node: T) -> Iterator[T]:
        """
        Yield all ancestors of a node (DFS).

        Cycle-safe (visited set).
        """
        visited = {node.id}
        stack = [node]

        while stack:
            current = stack.pop()

            for parent in self.parents_of(current):
                if parent.id in visited:
                    continue

                visited.add(parent.id)
                yield parent
                stack.append(parent)

    # ----------------------------------------------------------------------------------------------
    # Descendants
    # ----------------------------------------------------------------------------------------------
    def descendants(self, node: T) -> Iterator[T]:
        """
        Yield all descendants of a node (DFS).
        """
        visited = {node.id}
        stack = [node]

        while stack:
            current = stack.pop()

            for child in self.children_of(current):
                if child.id in visited:
                    continue

                visited.add(child.id)
                yield child
                stack.append(child)

    # ----------------------------------------------------------------------------------------------
    # Collectors
    # ----------------------------------------------------------------------------------------------
    def ancestors_list(self, node: T) -> list[T]:
        return list(self.ancestors(node))

    def descendants_list(self, node: T) -> list[T]:
        return list(self.descendants(node))

    def ancestors_ids(self, node: T) -> set[UUID]:
        return {n.id for n in self.ancestors(node)}

    def descendants_ids(self, node: T) -> set[UUID]:
        return {n.id for n in self.descendants(node)}

    # ----------------------------------------------------------------------------------------------
    # Predicates
    # ----------------------------------------------------------------------------------------------
    def is_ancestor(self, node: T, candidate: T) -> bool:
        """
        True if candidate is an ancestor of node.
        """
        return any(candidate.id == n.id for n in self.ancestors(node))

    def is_descendant(self, node: T, candidate: T) -> bool:
        return any(candidate.id == n.id for n in self.descendants(node))

    # ----------------------------------------------------------------------------------------------
    # Cycle detection
    # ----------------------------------------------------------------------------------------------
    def would_create_cycle(self, parent: T, child: T) -> bool:
        """
        Check if adding edge parent -> child would introduce a cycle.

        Equivalent to: parent is already in descendants(child)
        """
        return any(parent.id == n.id for n in self.descendants(child))
