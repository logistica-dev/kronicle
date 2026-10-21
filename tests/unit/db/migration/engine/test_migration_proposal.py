# tests/unit/db/migration/engine/test_migration_proposal.py
"""Unit tests for kronicle.db.migration.engine.migration_proposal."""

from unittest.mock import MagicMock, patch

from kronicle.db.migration.engine.migration_proposal import MigrationProposal
from kronicle.db.migration.engine.operations import (
    CreateSchemaOp,
    DropTableOp,
    SafetyLevel,
)

# ==================================================================================================
# _log_operation (logging level follows the safety level)
# ==================================================================================================


def _proposal() -> MigrationProposal:
    """Bypass __init__ (which needs a live connection) — these tests only exercise logging."""
    return MigrationProposal.__new__(MigrationProposal)


def test_log_operation_safe_uses_log_i():
    proposal = _proposal()
    with patch("kronicle.db.migration.engine.migration_proposal.log_i") as log_i:
        proposal._log_operation(CreateSchemaOp(schema="core"))
    log_i.assert_called_once()
    assert "SAFE -> create_schema:core" in log_i.call_args.args[1]


def test_log_operation_warning_uses_log_w():
    proposal = _proposal()
    op = MagicMock()
    op.safety.level = SafetyLevel.WARNING
    op.describe.return_value = "warn_op"
    with patch("kronicle.db.migration.engine.migration_proposal.log_w") as log_w:
        proposal._log_operation(op)
    log_w.assert_called_once_with("migration", "WARN -> warn_op")


def test_log_operation_destructive_uses_log_e():
    proposal = _proposal()
    with patch("kronicle.db.migration.engine.migration_proposal.log_e") as log_e:
        proposal._log_operation(DropTableOp(schema="core", table="legacy"))
    log_e.assert_called_once()
    assert "DEST -> drop_table:core.legacy" in log_e.call_args.args[1]
