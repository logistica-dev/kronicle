# kronicle/repo/hierarchy/hierarchy_service.py
from typing import TypeVar
from uuid import UUID

from sqlalchemy.orm import Session

from kronicle.db.base.kronicle_base import KronicleBase
from kronicle.repo.hierarchy.hierarchy_engine import HierarchyEngine
from kronicle.repo.hierarchy.hierarchy_repo import KronicleHierarchyRepo

N = TypeVar("N", bound=KronicleBase)


class HierarchyService:
    """
    High-level facade for hierarchy operations.

    Sessions are owned by callers: every method takes an explicit db session,
    so reads observe the caller's transaction and writes are checked and
    applied atomically within it.

    This is the only class that application code should use directly.
    """

    def __init__(
        self,
        repo: KronicleHierarchyRepo,
        max_parents: int | None = 1,
    ):
        self.repo = repo
        self.max_parents = max_parents

    def _engine(self, db: Session) -> HierarchyEngine:
        """Build a traversal engine bound to a specific session."""
        repo = self.repo
        return HierarchyEngine(
            parents_of=lambda node: repo.list_parents(db, node),
            children_of=lambda node: repo.list_children(db, node),
        )

    def list_parents(self, db: Session, node: N) -> list[N]:
        """
        List the direct parents of a node.
        """
        return self.repo.list_parents(db, node)

    def list_children(self, db: Session, node: N) -> list[N]:
        """
        List the direct children of a node.
        """
        return self.repo.list_children(db, node)

    def add_parent(self, db: Session, parent: N, child: N) -> None:
        """
        Safely add a parent -> child relationship.

        The cycle check, the max-parents check and the insert all run within
        the caller's transaction, observing a single consistent state.
        """
        engine = self._engine(db)
        if engine.would_create_cycle(parent, child):
            raise ValueError("Cycle detected")

        if self.max_parents is not None:
            parents = list(engine.parents_of(child))
            if len(parents) >= self.max_parents:
                raise ValueError("Max parents exceeded")

        self.repo.add_parent(db, parent, child)

    def remove_parent(self, db: Session, parent: N, child: N) -> None:
        """
        Remove a parent -> child relationship.
        """
        self.repo.remove_parent(db, parent, child)

    def ancestors(self, db: Session, node: N) -> list[N]:
        """
        Return all ancestors of a node.
        """
        return self._engine(db).ancestors_list(node)

    def descendants(self, db: Session, node: N) -> list[N]:
        """
        Return all descendants of a node.
        """
        return self._engine(db).descendants_list(node)

    def ancestors_ids(self, db: Session, node: KronicleBase) -> set[UUID]:
        """
        Return all ancestor IDs of a node.
        """
        return self._engine(db).ancestors_ids(node)

    def descendants_ids(self, db: Session, node: KronicleBase) -> set[UUID]:
        """
        Return all descendant IDs of a node.
        """
        return self._engine(db).descendants_ids(node)

    def descendant_closure(self, db: Session, nodes: list[N]) -> set[N]:
        """
        Expand multiple roots into full closure set.

        Used heavily by RBAC resolution.
        """
        engine = self._engine(db)
        result = {}
        for node in nodes:
            for d in engine.descendants(node):
                result[d.id] = d
            result[node.id] = node
        return set(result.values())
