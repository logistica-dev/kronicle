# kronicle/db/migration/migration_orchestrator.py
"""
MigrationOrchestrator: drives the provisioners to bring the database fully
in line with the metadata model.

Responsibilities:
  - infra  : DbProvisioner.run_once()        (DB, owner roles, schemas, TimescaleDB)
  - schema : RbacSchemasProvisioner.run_once() looped until converged (core+rbac)

Both provisioners implement the shared BaseProvisioner contract, whose concrete
run_once() runs analyze -> ask_validation -> backup -> execute_plan -> run_post_analysis
and returns an ApplyResult with a status: ``ok`` / ``leftovers`` / ``error`` / ``aborted``.

Why schema convergence is a loop:
  On a fresh database the diff/plan engine needs two passes to converge — it creates
  tables + indexes first, then the cross-table constraints (uniques, checks, FKs) that
  depend on all tables existing. A single run_once() therefore legitimately returns
  ``leftovers`` (post-analysis found work still outstanding). The orchestrator simply
  repeats run_once() until it reports ``ok`` (converged).

Convergence policy:
  1. round 0   run with the operator's gate (or auto_approve) — user validates the plan
  2. round N>0 run with auto_approve_if_non_destructive=True — pure convergence tail
  3. loop until the provisioner returns ok (converged) or the user aborts

  The loop is bounded by max_iterations and a wall-clock deadline so it can never spin.
  Destructive operations only ever arise from user-validated intent in round 0 (a removed
  column/table in the models); the non-destructive guard means a convergence round can never
  silently drop anything — if one unexpectedly did, the runner falls back to prompting
  instead of auto-applying.

MigrationManager is intentionally NOT used here; it is kept as a fallback until it is
retired. DataMigrationManager (data schema, asyncpg) will be added as a later provisioner.
"""

from __future__ import annotations

import argparse
import os
import time
from dataclasses import dataclass, field
from pathlib import Path

from kronicle.db.migration.orchestrators.db_provisioner import DbProvisioner
from kronicle.db.migration.orchestrators.db_rbac_provisioner import RbacSchemasProvisioner
from kronicle.deps.settings import KronicleSettings
from kronicle.deps.settings_env import DBSettings
from kronicle.utils.dev_logs import log_d, log_i
from kronicle.utils.file_utils import load_env_file

mod = "migration_orchestrator"


class OrchestrationError(RuntimeError):
    """Raised when the orchestrated migration cannot reach a valid, verified state."""


@dataclass
class PassOutcome:
    """Outcome of one schema-provisioning pass within the convergence loop."""

    round: int
    applied_ops: int
    safety_level: str | None
    aborted: bool

    @property
    def converged(self) -> bool:
        return self.applied_ops == 0 and not self.aborted


@dataclass
class OrchestrationResult:
    """Full result of an orchestrated migration run."""

    infra_required_fixes: bool = False
    passes: list[PassOutcome] = field(default_factory=list)
    converged: bool = False
    verified: bool = False

    @property
    def total_applied_ops(self) -> int:
        return sum(p.applied_ops for p in self.passes)


class MigrationOrchestrator:
    """Run infra + schema provisioners to convergence, then post-analyse."""

    def __init__(
        self,
        db_settings: DBSettings,
        *,
        auto_approve: bool = False,
        max_iterations: int = 10,
        max_total_seconds: float | None = 600.0,
        infra_provisioner: DbProvisioner | None = None,
        schema_provisioner: RbacSchemasProvisioner | None = None,
    ):
        if max_iterations < 1:
            raise ValueError("max_iterations must be >= 1")
        self._db_settings = db_settings
        self.auto_approve = auto_approve
        self.max_iterations = max_iterations
        self.max_total_seconds = max_total_seconds

        self.infra = infra_provisioner or DbProvisioner(db_settings=db_settings)
        self.schema = schema_provisioner or RbacSchemasProvisioner(db_settings=db_settings)

    def run(self, *, auto_approve: bool | None = None, verbose: bool = True) -> OrchestrationResult:
        """Run infra provisioner, then the schema convergence loop."""
        auto_approve = self.auto_approve if auto_approve is None else auto_approve
        result = OrchestrationResult()

        # --------------------------------------------------------------
        # 1. INFRA (DB, roles, schemas, extension) — idempotent, one gated pass
        # --------------------------------------------------------------
        infra = self.infra.run_once(auto_approve=auto_approve, verbose=verbose)
        if infra.failed:
            raise OrchestrationError(f"Infra provision failed: {infra.message}")
        if infra.aborted:
            log_i(mod, "Infra provision aborted by user; leaving database unchanged.")
            return result
        result.infra_required_fixes = infra.applied_ops > 0

        # --------------------------------------------------------------
        # 2. SCHEMA CONVERGENCE (bounded loop over BaseProvisioner.run_once)
        #    Each run_once() finishes with run_post_analysis(), so it reports
        #    ok / leftovers; we simply repeat until it converges.
        # --------------------------------------------------------------
        deadline = (time.monotonic() + self.max_total_seconds) if self.max_total_seconds else None

        for round_no in range(1, self.max_iterations + 1):
            if deadline is not None and time.monotonic() > deadline:
                raise OrchestrationError(
                    f"Schema migration did not converge within {self.max_total_seconds}s "
                    f"(stopped after round {round_no})."
                )

            if round_no == 1:
                # Round 0: operator validates the (first, meaningful) plan.
                outcome = self.schema.run_once(auto_approve=auto_approve, verbose=verbose)
            else:
                # Convergence tail: auto-approve only if non-destructive (guarded).
                outcome = self.schema.run_once(auto_approve_if_non_destructive=True, verbose=verbose)

            result.passes.append(
                PassOutcome(
                    round=round_no,
                    applied_ops=outcome.applied_ops,
                    safety_level=outcome.safety_level,
                    aborted=outcome.aborted,
                )
            )

            if outcome.failed:
                raise OrchestrationError(f"Schema migration failed: {outcome.message}")

            if outcome.aborted:
                log_i(mod, "Schema migration aborted by user; leaving database unchanged.")
                return result

            if outcome.converged:
                result.converged = True
                log_i(mod, f"Schema migration converged after round {round_no}.")
                break
        else:
            raise OrchestrationError(f"Schema migration did not converge after {self.max_iterations} rounds.")

        # run_post_analysis() already ran as the last step of run_once(); a
        # converged result means the database matches the metadata model.
        result.verified = True
        log_i(mod, "Orchestrated migration complete and verified.")
        return result


# ======================================================================================
# CLI entrypoint
# ======================================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kronicle migration orchestrator")
    parser.add_argument("--secrets", default=None, help="Path to a .secrets file to load")
    parser.add_argument(
        "--auto-approve",
        action="store_true",
        help="Approve infra prerequisites + the first migration plan without y/n prompts",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=10,
        help="Safety cap on convergence rounds (default 10)",
    )
    parser.add_argument(
        "--max-total-seconds",
        type=float,
        default=600.0,
        help="Safety deadline for the whole schema run in seconds (0 disables)",
    )
    args = parser.parse_args()

    secrets_env = os.environ.get("KRONICLE_SECRETS_PATH")
    secrets_default = Path(__file__).resolve().parent.parent.parent.parent.parent / ".conf" / ".secrets"
    secrets_path = Path(args.secrets) if args.secrets else Path(secrets_env) if secrets_env else secrets_default
    if secrets_path.exists():
        load_env_file(secrets_path)
        log_d(mod, "Env var loaded")
    else:
        log_d(mod, "Secrets file not found", secrets_path)

    settings = KronicleSettings()
    orchestrator = MigrationOrchestrator(
        db_settings=settings.db,
        auto_approve=args.auto_approve,
        max_iterations=args.max_iterations,
        max_total_seconds=args.max_total_seconds or None,
    )
    orchestrator.run()
