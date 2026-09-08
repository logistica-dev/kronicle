# tests/unit/db/migration/orchestrators/test_migration_orchestrator.py
"""Unit tests for kronicle.db.migration.orchestrators.migration_orchestrator."""

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import Column, Integer

from kronicle.db.migration.engine.migration_plan import MigrationPlan
from kronicle.db.migration.engine.operations import AddColumnOp
from kronicle.db.migration.orchestrators import migration_orchestrator as mo
from kronicle.db.migration.orchestrators.db_data_provisioner import ChannelDrift, DataSchemaProvisioner
from kronicle.db.migration.orchestrators.db_provisioner import DbProvisioner
from kronicle.db.migration.orchestrators.db_rbac_provisioner import RbacSchemasProvisioner
from kronicle.db.migration.orchestrators.migration_orchestrator import (
    MigrationOrchestrator,
    OrchestrationError,
    OrchestrationResult,
    PassOutcome,
)
from kronicle.db.migration.orchestrators.provisioner_base import ApplyResult

from .conftest import make_db_settings

# ==================================================================================================
# Helpers
# ==================================================================================================


def _ok(applied_ops: int = 0, safety_level: str | None = None):
    return ApplyResult(status="ok", applied_ops=applied_ops, safety_level=safety_level)


def _leftovers(applied_ops: int = 0):
    return ApplyResult(status="leftovers", applied_ops=applied_ops)


def _orchestrator(infra, schema, data, **kwargs):
    return MigrationOrchestrator(
        make_db_settings(),
        infra_provisioner=infra,
        schema_provisioner=schema,
        data_provisioner=data,
        **kwargs,
    )


def _plan():
    return MigrationPlan.build(
        [AddColumnOp(schema="core", table="some_table", column_name="col", column_def=Column("col", Integer))]
    )


class _AdvancingClock:
    _now = 1000.0

    def monotonic(self):
        self._now += 2000.0
        return self._now


# ==================================================================================================
# Construction
# ==================================================================================================


def test_init_raises_when_max_iterations_invalid():
    with pytest.raises(ValueError, match="max_iterations"):
        MigrationOrchestrator(make_db_settings(), max_iterations=0)


def test_init_defaults_build_all_provisioners():
    orchestrator = MigrationOrchestrator(make_db_settings())
    assert isinstance(orchestrator.infra, DbProvisioner)
    assert isinstance(orchestrator.schema, RbacSchemasProvisioner)
    assert isinstance(orchestrator.data, DataSchemaProvisioner)
    assert orchestrator.max_iterations == 10
    assert orchestrator.max_total_seconds == 600.0
    assert orchestrator.auto_approve is False


# ==================================================================================================
# run()
# ==================================================================================================


def test_run_full_success():
    infra, schema, data = MagicMock(), MagicMock(), MagicMock()
    infra.run_once.return_value = _ok(applied_ops=2, safety_level="safe")
    schema.run_once.return_value = _ok(applied_ops=1)
    data.run_once.return_value = _ok()
    orchestrator = _orchestrator(infra, schema, data, auto_approve=True)

    result = orchestrator.run()

    assert result.converged is True
    assert result.verified is True
    assert result.infra_required_fixes is True
    assert result.total_applied_ops == 1
    assert [p.round for p in result.passes] == [1, 1]


def test_run_infra_failure_raises():
    infra, schema, data = MagicMock(), MagicMock(), MagicMock()
    infra.run_once.return_value = ApplyResult(status="error", message="boom")
    orchestrator = _orchestrator(infra, schema, data)

    with pytest.raises(OrchestrationError, match="Infra provision failed: boom"):
        orchestrator.run()

    schema.run_once.assert_not_called()


def test_run_infra_aborted_returns_untouched_result():
    infra, schema, data = MagicMock(), MagicMock(), MagicMock()
    infra.run_once.return_value = ApplyResult(status="aborted")
    orchestrator = _orchestrator(infra, schema, data)

    result = orchestrator.run()

    assert result.converged is False
    assert result.verified is False
    assert result.passes == []
    schema.run_once.assert_not_called()


def test_run_schema_aborted_returns_result():
    infra, schema, data = MagicMock(), MagicMock(), MagicMock()
    infra.run_once.return_value = _ok()
    schema.run_once.return_value = ApplyResult(status="aborted")
    orchestrator = _orchestrator(infra, schema, data)

    result = orchestrator.run(auto_approve=True)

    assert result.converged is False
    assert result.passes == []
    data.run_once.assert_not_called()


def test_run_two_round_schema_convergence_tail():
    infra, schema, data = MagicMock(), MagicMock(), MagicMock()
    infra.run_once.return_value = _ok()
    schema.run_once.side_effect = [_leftovers(applied_ops=3), _ok(applied_ops=2, safety_level="safe")]
    data.run_once.return_value = _ok()
    orchestrator = _orchestrator(infra, schema, data, auto_approve=True)

    result = orchestrator.run()

    assert result.converged is True
    assert len(result.passes) == 3
    assert [p.round for p in result.passes] == [1, 2, 1]
    assert result.total_applied_ops == 5
    assert schema.run_once.call_args_list[0].kwargs["auto_approve"] is True
    assert schema.run_once.call_args_list[1].kwargs == {"auto_approve_if_non_destructive": True, "verbose": True}


def test_run_schema_non_convergence_raises():
    infra, schema, data = MagicMock(), MagicMock(), MagicMock()
    infra.run_once.return_value = _ok()
    schema.run_once.return_value = _leftovers(applied_ops=1)
    orchestrator = _orchestrator(infra, schema, data, max_iterations=3)

    with pytest.raises(OrchestrationError, match="schema did not converge after 3 rounds"):
        orchestrator.run()


def test_run_data_aborted_returns_result():
    infra, schema, data = MagicMock(), MagicMock(), MagicMock()
    infra.run_once.return_value = _ok()
    schema.run_once.return_value = _ok(applied_ops=2)
    data.run_once.return_value = ApplyResult(status="aborted")
    orchestrator = _orchestrator(infra, schema, data)

    result = orchestrator.run()

    assert result.converged is False
    assert len(result.passes) == 1
    assert result.passes[0].applied_ops == 2


def test_run_data_non_convergence_raises():
    infra, schema, data = MagicMock(), MagicMock(), MagicMock()
    infra.run_once.return_value = _ok()
    schema.run_once.return_value = _ok()
    data.run_once.return_value = _leftovers(applied_ops=1)
    orchestrator = _orchestrator(infra, schema, data, max_iterations=2)

    with pytest.raises(OrchestrationError, match="data did not converge after 2 rounds"):
        orchestrator.run()


# ==================================================================================================
# _converge
# ==================================================================================================


def test_converge_converged_first_round():
    provisioner = MagicMock()
    provisioner.run_once.return_value = _ok()
    orchestrator = _orchestrator(MagicMock(), MagicMock(), MagicMock())

    outcome = orchestrator._converge("schema", provisioner, auto_approve=False, verbose=True)

    assert outcome == {
        "passes": [PassOutcome(round=1, applied_ops=0, safety_level=None, aborted=False)],
        "converged": True,
        "aborted": False,
    }
    provisioner.run_once.assert_called_once_with(auto_approve=False, verbose=True)


def test_converge_aborted_returns_aborted():
    provisioner = MagicMock()
    provisioner.run_once.return_value = ApplyResult(status="aborted")
    orchestrator = _orchestrator(MagicMock(), MagicMock(), MagicMock())

    outcome = orchestrator._converge("schema", provisioner, auto_approve=False, verbose=True)

    assert outcome["aborted"] is True
    assert outcome["converged"] is False
    assert outcome["passes"][0].aborted is True


def test_converge_raises_when_provisioner_fails():
    provisioner = MagicMock()
    provisioner.run_once.return_value = ApplyResult(status="error", message="exploded")
    orchestrator = _orchestrator(MagicMock(), MagicMock(), MagicMock())

    with pytest.raises(OrchestrationError, match="schema migration failed: exploded"):
        orchestrator._converge("schema", provisioner, auto_approve=False, verbose=True)


def test_converge_raises_when_deadline_exceeded():
    provisioner = MagicMock()
    provisioner.run_once.return_value = _leftovers(applied_ops=1)
    orchestrator = _orchestrator(MagicMock(), MagicMock(), MagicMock(), max_total_seconds=600.0)

    with patch.object(mo, "time", _AdvancingClock()):
        with pytest.raises(OrchestrationError, match="did not converge within 600.0"):
            orchestrator._converge("schema", provisioner, auto_approve=False, verbose=True)
    provisioner.run_once.assert_not_called()


# ==================================================================================================
# validate()
# ==================================================================================================


def test_validate_ok_when_everything_aligned():
    infra, schema, data = MagicMock(), MagicMock(), MagicMock()
    infra._missing = {}
    schema._tracking_fixes = []
    schema._plan = None
    data._tracking_missing = False
    data._metadata_missing = False
    data.channels = []
    orchestrator = _orchestrator(infra, schema, data)

    result = orchestrator.validate()

    assert result.converged is True
    assert result.verified is True
    infra.analyze.assert_called_once()
    schema.analyze.assert_called_once()
    data.analyze.assert_called_once()


def test_validate_raises_with_full_report():
    infra, schema, data = MagicMock(), MagicMock(), MagicMock()
    infra._missing = {"db": ["create database kronicle"]}
    schema._tracking_fixes = ["core"]
    schema._plan = _plan()
    data._tracking_missing = True
    data._metadata_missing = True
    data.channels = [
        ChannelDrift(table="channel_x", missing_columns=["received_at"], pk_columns=["time"], is_hypertable=False)
    ]
    orchestrator = _orchestrator(infra, schema, data)

    with pytest.raises(OrchestrationError) as exc_info:
        orchestrator.validate()

    message = str(exc_info.value)
    assert "[infra:db] create database kronicle" in message
    assert "[schema] create tracking tables in 'core'" in message
    assert "[schema:core]" in message
    assert "[data] create migration tracking tables" in message
    assert "[data] create ChannelMetadata table" in message
    assert "[data] channel_x: missing received_at; PK ['time'] != ['time','row_id']; not a hypertable" in message


# ==================================================================================================
# Result dataclasses
# ==================================================================================================


def test_pass_outcome_converged_property():
    assert PassOutcome(round=1, applied_ops=0, safety_level=None, aborted=False).converged is True
    assert PassOutcome(round=1, applied_ops=2, safety_level=None, aborted=False).converged is False
    assert PassOutcome(round=1, applied_ops=0, safety_level=None, aborted=True).converged is False


def test_orchestration_result_total_applied_ops():
    result = OrchestrationResult()
    result.passes = [
        PassOutcome(round=1, applied_ops=3, safety_level=None, aborted=False),
        PassOutcome(round=2, applied_ops=0, safety_level=None, aborted=False),
    ]
    assert result.total_applied_ops == 3
    assert result.converged is False
    assert result.infra_required_fixes is False
