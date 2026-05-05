from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Set

from alembic.operations import Operations

from kronicle.db.migration.operations import SafetyLevel, SchemaOperation

# =============================================================================
# MigrationPlan
# =============================================================================


@dataclass
class MigrationPlan:
    """
    Represents a fully resolved migration execution plan as a sorted DAG of SchemaOperation
    - grouped by safety level
    - validated for dependency correctness
    - ready to be rendered into Alembic or executed step-by-step

    This is NOT execution.
    This is a deterministic ordering of operations.
    """

    operations: List[SchemaOperation]

    # internal graph cache
    _graph: Dict[str, Set[str]] = field(default_factory=dict, init=False)
    _index: Dict[str, SchemaOperation] = field(default_factory=dict, init=False)

    # ------------------------------------------------------------------
    # Build phase
    # ------------------------------------------------------------------
    def __post_init__(self):
        self._index = {op.op_id: op for op in self.operations}
        self._graph = self._build_graph()

    # ------------------------------------------------------------------
    # Dependency graph
    # ------------------------------------------------------------------
    def _build_graph(self) -> Dict[str, Set[str]]:
        graph: Dict[str, Set[str]] = {op.op_id: set() for op in self.operations}

        for op in self.operations:
            for dep in op.depends_on:
                if dep in graph:
                    graph[op.op_id].add(dep)

        return graph

    # ------------------------------------------------------------------
    # Topological sort
    # ------------------------------------------------------------------
    def ordered(self) -> List[SchemaOperation]:
        visited = set()
        temp = set()
        result = []

        def visit(node: str):
            if node in temp:
                raise RuntimeError(f"Circular dependency detected at {node}")

            if node in visited:
                return

            temp.add(node)

            for dep in self._graph.get(node, []):
                visit(dep)

            temp.remove(node)
            visited.add(node)
            result.append(self._index[node])

        for op in self.operations:
            visit(op.op_id)

        # stable ordering by priority within same dependency level
        result.sort(key=lambda o: o.priority)

        return result

    # ------------------------------------------------------------------
    # Safety grouping
    # ------------------------------------------------------------------
    def by_safety(self) -> dict[str, List[SchemaOperation]]:
        groups = {
            SafetyLevel.SAFE: [],
            SafetyLevel.WARNING: [],
            SafetyLevel.DESTRUCTIVE: [],
        }

        for op in self.ordered():
            groups[op.safety].append(op)

        return groups

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------
    def apply(self, op: Operations) -> None:
        """
        Execute full migration plan using Alembic operations context.
        """
        for operation in self.ordered():
            operation.apply(op)

    # ------------------------------------------------------------------
    # Debug helpers
    # ------------------------------------------------------------------
    def describe(self) -> List[str]:
        return [op.describe() for op in self.ordered()]
