# kronicle/db/migration/bootstrap_report.py
from __future__ import annotations

from dataclasses import dataclass

from kronicle.db.migration.engine.operations import DbStructureOperation


# =============================================================================
# Diagnostic model
# =============================================================================
@dataclass(frozen=True)
class BootstrapIssue:
    level: str  # "error" | "warning" | "info"
    message: str
    schema: str | None = None
    table: str | None = None


# =============================================================================
# Bootstrap report (diagnostics only)
# =============================================================================
class BootstrapReport:
    """
    Collects:
    - validation issues (errors / warnings / info)
    - NO planning logic
    """

    def __init__(self):
        self.issues: list[BootstrapIssue] = []
        self.operations: list[DbStructureOperation] = []

    # ------------------------------------------------------------------
    # Issues
    # ------------------------------------------------------------------
    def add_error(self, msg: str, *, schema: str | None = None, table: str | None = None):
        self.issues.append(BootstrapIssue("error", msg, schema, table))

    def add_warning(self, msg: str, *, schema: str | None = None, table: str | None = None):
        self.issues.append(BootstrapIssue("warning", msg, schema, table))

    def add_info(self, msg: str, *, schema: str | None = None, table: str | None = None):
        self.issues.append(BootstrapIssue("info", msg, schema, table))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @property
    def errors(self) -> list[str]:
        return [i.message for i in self.issues if i.level == "error"]

    @property
    def warnings(self) -> list[str]:
        return [i.message for i in self.issues if i.level == "warning"]

    @property
    def is_valid(self) -> bool:
        return not any(i.level == "error" for i in self.issues)

    # ------------------------------------------------------------------
    # Execution helpers
    # ------------------------------------------------------------------
    def raise_if_invalid(self):
        if not self.is_valid:
            raise RuntimeError(self.format())

    def format(self) -> str:
        parts = []

        if self.errors:
            parts.append("Errors:\n" + "\n".join(self.errors))

        if self.warnings:
            parts.append("Warnings:\n" + "\n".join(self.warnings))

        return "\n\n".join(parts)
