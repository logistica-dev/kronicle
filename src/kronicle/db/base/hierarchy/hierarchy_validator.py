# kronicle/db/base/hierarchy/hierarchy_validator.py

from typing import Generic, TypeVar

from kronicle.db.base.hierarchy.hierarchy_engine import HierarchyEngine
from kronicle.db.base.kronicle_base import KronicleBase

T = TypeVar("T", bound=KronicleBase)


class HierarchyValidator(Generic[T]):
    """
    Validates structural constraints of a hierarchy.

    Responsibilities:
      - enforce max parent constraints
      - detect invalid configurations
      - ensure graph integrity before runtime traversal logic
    """

    def __init__(self, engine: HierarchyEngine[T], max_parents: int | None = 1):
        self.engine = engine
        self.max_parents = max_parents

    def validate_add_parent(self, parent: T, child: T) -> None:
        # --- No self-loop ---
        if parent.id == child.id:
            raise ValueError("A node cannot be its own parent")

        # --- Cycle detection ---
        if self.engine.would_create_cycle(parent, child):
            raise ValueError("Adding this relation would create a cycle")

        # --- Max parents constraint (tree vs DAG) ---
        if self.max_parents is not None:
            current_parents = list(self.engine.parents_of(child))
            if len(current_parents) >= self.max_parents:
                raise ValueError("Max number of parents exceeded")
