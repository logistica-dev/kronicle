# kronicle/db/migration/migration_plan.py
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Set

from kronicle.db.migration.operations import DbStructureOperation, SafetyLevel


# =============================================================================
# MigrationPlan
# =============================================================================
@dataclass
class MigrationPlan:
    """
    Fully resolved, ordered migration execution plan.

    Responsibilities:
    - resolve dependency graph
    - apply deterministic ordering (priority + DAG)
    - classify safety levels
    - provide execution interface
    """

    operations: List[DbStructureOperation]

    # computed
    ordered_operations: List[DbStructureOperation] = field(init=False, default_factory=list)
    by_safety: Dict[str, List[DbStructureOperation]] = field(init=False, default_factory=dict)

    # --------------------------------------------------------------------------
    # Build entry point
    # --------------------------------------------------------------------------
    @classmethod
    def build(cls, ops: Iterable[DbStructureOperation]) -> "MigrationPlan":
        plan = cls(list(ops))
        plan._resolve()
        plan._classify()
        return plan

    # --------------------------------------------------------------------------
    # DAG resolution (priority-aware topological sort)
    # --------------------------------------------------------------------------
    def _resolve(self) -> None:
        ops = self.operations

        id_map: Dict[str, DbStructureOperation] = {op.op_id: op for op in ops}

        graph: Dict[str, Set[str]] = defaultdict(set)
        indegree: Dict[str, int] = defaultdict(int)

        # ----------------------------------------------------------------------
        # Build graph
        # ----------------------------------------------------------------------
        for op in ops:
            for dep in op.depends_on:
                graph[dep].add(op.op_id)
                indegree[op.op_id] += 1

        # ----------------------------------------------------------------------
        # Priority-aware queue (stable ordering)
        # ----------------------------------------------------------------------
        def sort_key(op_id: str):
            op = id_map[op_id]
            return (op.priority, op.op_id)

        queue = deque(
            sorted(
                [op_id for op_id in id_map if indegree[op_id] == 0],
                key=sort_key,
            )
        )

        ordered: List[DbStructureOperation] = []

        # ----------------------------------------------------------------------
        # Kahn algorithm (stable)
        # ----------------------------------------------------------------------
        while queue:
            current = queue.popleft()
            ordered.append(id_map[current])

            for nxt in graph[current]:
                indegree[nxt] -= 1

                if indegree[nxt] == 0:
                    queue.append(nxt)

            # keep queue deterministic after each step
            queue = deque(sorted(queue, key=sort_key))

        # ----------------------------------------------------------------------
        # cycle detection
        # ----------------------------------------------------------------------
        if len(ordered) != len(ops):
            missing = set(id_map.keys()) - {o.op_id for o in ordered}
            raise RuntimeError(f"Cyclic or unresolved dependencies: {missing}")

        self.ordered_operations = ordered

    # --------------------------------------------------------------------------
    # Safety classification
    # --------------------------------------------------------------------------
    def _classify(self) -> None:
        grouped: Dict[str, List[DbStructureOperation]] = defaultdict(list)

        for op in self.ordered_operations:
            grouped[op.safety.level].append(op)

        self.by_safety = grouped

    # --------------------------------------------------------------------------
    # Revision (deterministic hash of the ordered operations)
    # --------------------------------------------------------------------------
    @property
    def revision(self) -> str:
        import hashlib

        combined = "|".join(op.describe() for op in self.ordered_operations)
        return hashlib.sha256(combined.encode()).hexdigest()[:12]

    # --------------------------------------------------------------------------
    # Schemas involved in this plan
    # --------------------------------------------------------------------------
    @property
    def schemas(self) -> set[str]:
        return {getattr(op, "schema", "") for op in self.ordered_operations if getattr(op, "schema", None)}

    # --------------------------------------------------------------------------
    # Query helpers
    # --------------------------------------------------------------------------
    def safe_ops(self) -> List[DbStructureOperation]:
        return self.by_safety.get(SafetyLevel.SAFE, [])

    def warning_ops(self) -> List[DbStructureOperation]:
        return self.by_safety.get(SafetyLevel.WARNING, [])

    def destructive_ops(self) -> List[DbStructureOperation]:
        return self.by_safety.get(SafetyLevel.DESTRUCTIVE, [])

    # --------------------------------------------------------------------------
    # Execution
    # --------------------------------------------------------------------------
    def apply(self, alembic_ops) -> None:
        """
        Execute all operations using Alembic Operations context.
        """
        for op in self.ordered_operations:
            op.apply(alembic_ops)

    # --------------------------------------------------------------------------
    # Debug
    # --------------------------------------------------------------------------
    def summary(self) -> dict:
        return {
            "total": len(self.operations),
            "ordered": [op.describe() for op in self.ordered_operations],
            "safe": len(self.safe_ops()),
            "warning": len(self.warning_ops()),
            "destructive": len(self.destructive_ops()),
        }
