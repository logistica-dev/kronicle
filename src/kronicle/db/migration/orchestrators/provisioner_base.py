# kronicle/db/migration/provisioner_base.py
"""
Abstract base for all Kronicle provisioners (infra, core+rbac schemas, and the
future data schema). It defines the uniform contract a provisioner exposes to
the MigrationOrchestrator, and the shared helpers/communication types.

Every provisioner implements the same workflow, driven by the concrete
``run_once()``:

    analyze()          -> read-only: derive the desired changes (plan / prereqs)
    ask_validation()   -> present the analysis; confirm with the user (or auto-approve)
    backup()           -> safeguard snapshot before mutating
    execute_plan()     -> apply the changes (side effects)
    run_post_analysis()-> verify the execution; classify converged / leftovers / error

``run_once()`` runs those steps in order and returns an :class:`ApplyResult`
describing the outcome. It also wires error handling: on failure during
backup/execute it calls :meth:`restore_backup` and returns a status ``error``
result (it does not re-raise, so the orchestrator can react uniformly).

The orchestrator simply loops ``run_once()`` until the returned result reports
convergence, so the exact same driver works for every provisioner.
"""

from __future__ import annotations

import abc
from datetime import datetime, timezone
from pathlib import Path

STAMP_FMT = "%Y%m%d_%H%M%S"


def _now_stamp() -> str:
    return datetime.now(timezone.utc).strftime(STAMP_FMT)


def backup_path(prefix: str, variant: str, ts: str | None = None) -> Path:
    """Shared backup-file path: ``<prefix_dir>/<prefix_name>_<variant>_<ts>.dump``.

    ``ts`` defaults to the current UTC time so every sub-backup taken in one
    workflow shares a common timestamp when passed explicitly.
    """
    p = Path(prefix)
    ts = ts or _now_stamp()
    return p.parent / f"{p.name}_{variant}_{ts}.dump"


class ApplyResult:
    """
    Uniform outcome of a single ``run_once()`` pass, consumed by the orchestrator.

    ``status`` is the primary communication channel:
      - ``ok``        : execution completed and post-analysis found no leftovers
                        (``converged`` == True).
      - ``leftovers`` : execution completed but the analysis still reports work
                        remaining (a further ``run_once()`` is needed).
      - ``error``     : the pass failed (backup restored if one was taken).
      - ``aborted``   : the user declined validation; nothing was executed.
    """

    __slots__ = ("status", "applied_ops", "safety_level", "revision", "message")

    def __init__(
        self,
        *,
        status: str,
        applied_ops: int = 0,
        safety_level: str | None = None,
        revision: str | None = None,
        message: str | None = None,
    ):
        if status not in ("ok", "leftovers", "error", "aborted"):
            raise ValueError(f"invalid status: {status!r}")
        self.status = status
        self.applied_ops = applied_ops
        self.safety_level = safety_level
        self.revision = revision
        self.message = message

    @property
    def converged(self) -> bool:
        return self.status == "ok"

    @property
    def aborted(self) -> bool:
        return self.status == "aborted"

    @property
    def failed(self) -> bool:
        return self.status == "error"

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"ApplyResult(status={self.status!r}, applied_ops={self.applied_ops})"


class BaseProvisioner(abc.ABC):
    """Contract + shared workflow driver for every provisioner."""

    # -- workflow steps (implemented by subclasses) ----------------------

    @abc.abstractmethod
    def analyze(self, **kwargs) -> None:
        """Read-only: derive the desired changes (plan / missing prereqs)."""

    @abc.abstractmethod
    def ask_validation(self, **kwargs) -> bool:
        """Present the analysis and request confirmation. Returns True to proceed.

        Should be a no-op returning True when auto-approve is requested (or when
        an ``auto_approve_if_non_destructive`` guard passes for a non-destructive
        plan)."""

    @abc.abstractmethod
    @abc.abstractmethod
    def backup(self) -> Path | str | None:
        """Safeguard snapshot before mutating. Return the backup file path (None if n/a)."""

    @abc.abstractmethod
    def restore_backup(self, backup_file: Path | str | None) -> None:
        """Roll the database back to the saved backup after a failed execution."""

    @abc.abstractmethod
    def execute_plan(self) -> None:
        """Apply the desired changes (side effects only).

        Subclasses should record the number of changes applied on ``self._applied_ops``
        so ``run_once()`` can surface it on the returned :class:`ApplyResult`."""

    @abc.abstractmethod
    def run_post_analysis(self) -> bool:
        """Verify the execution; return True if converged (no leftovers), False otherwise."""

    # -- shared workflow driver ------------------------------------------

    def run_once(self, **kwargs) -> ApplyResult:
        """Analyze -> validate -> backup -> execute -> post-analyse, with restore on error.

        ``analyze()`` must set ``self._has_work`` (True when there are changes to
        apply). When it is False the run is a read-only no-op and immediately
        returns an ``ok`` (converged) result without validating/backing up.
        """
        self.analyze(**kwargs)

        if not getattr(self, "_has_work", False):
            run_post = getattr(self, "run_post_analysis", None)
            if run_post is not None:
                run_post()
            return ApplyResult(status="ok")

        if not self.ask_validation(**kwargs):
            return ApplyResult(status="aborted")

        backup_file: Path | str | None = None
        try:
            backup_file = self.backup()
            self.execute_plan(**kwargs)
            converged = self.run_post_analysis()
        except Exception as e:
            self.restore_backup(backup_file)
            return ApplyResult(status="error", message=str(e))

        print()
        return ApplyResult(
            status="ok" if converged else "leftovers",
            applied_ops=getattr(self, "_applied_ops", 0),
            safety_level=getattr(self, "_safety", None),
            revision=getattr(self, "_revision", None),
        )
