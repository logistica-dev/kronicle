# tests/unit/db/migration/orchestrators/test_db_rbac_provisioner.py
"""Unit tests for kronicle.db.migration.orchestrators.db_rbac_provisioner."""

import os
from pathlib import Path
from subprocess import CalledProcessError
from unittest.mock import MagicMock, call, patch

import pytest
from sqlalchemy import Column, Integer
from sqlalchemy.dialects import postgresql

from kronicle.db.migration.engine.migration_plan import MigrationPlan
from kronicle.db.migration.engine.operations import (
    AddColumnOp,
    AddUniqueConstraintOp,
    DropIndexOp,
    SafetyLevel,
)
from kronicle.db.migration.orchestrators import db_rbac_provisioner as dpr
from kronicle.db.migration.orchestrators.db_rbac_provisioner import (
    RbacSchemasProvisioner,
    _build_fk_checks,
    _build_orphan_delete_sql,
    _delete_orphans,
    _FkCheck,
)
from kronicle.db.migration.persistence.schema_migration_state import (
    CoreSchemaMigrationState,
    RbacSchemaMigrationState,
)
from kronicle.db.rbac.models.rbac_subject import RbacSubject
from kronicle.deps.settings_env import KRONICLE_RBAC_BACKUP

from .conftest import make_db_settings

# ==================================================================================================
# Helpers
# ==================================================================================================


def _completed(stdout: str = ""):
    return MagicMock(stdout=stdout, stderr="")


def _engine(row):
    """A mocked engine whose connection returns ``row`` from ``execute().first()``."""
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.first.return_value = row
    engine.connect.return_value.__enter__.return_value = conn
    return engine


def _engine_with_first(results):
    """A mocked engine whose connection returns ``results`` sequentially from ``first()``."""
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.first.side_effect = results
    engine.connect.return_value.__enter__.return_value = conn
    engine.begin.return_value.__enter__.return_value = conn
    return engine


def _exec_engine():
    """A mocked engine exposing a ``begin()`` connection (no ``execute().first()``)."""
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    return engine


def _provisioner(**settings_kwargs):
    return RbacSchemasProvisioner(db_settings=make_db_settings(**settings_kwargs))


def _add_col_op(schema="core"):
    return AddColumnOp(schema=schema, table="some_table", column_name="col", column_def=Column("col", Integer))


def _drop_idx_op():
    return DropIndexOp(schema="core", table="some_table", index_name="ix_col")


def _add_unique_op():
    return AddUniqueConstraintOp(schema="core", table="some_table", constraint_name="uq_col", columns=("col",))


class _StateRow:
    def __init__(self, revision="rev-1", schema_hash="storedhash"):
        self.revision = revision
        self.schema_hash = schema_hash


# ==================================================================================================
# Module-level helpers
# ==================================================================================================


def test_build_fk_checks_empty_for_unknown_schemas():
    assert _build_fk_checks(["no_such_schema"], MigrationPlan.build([])) == []


def test_build_fk_checks_collects_rbac_foreign_keys():
    checks = _build_fk_checks(["rbac"], MigrationPlan.build([]))
    assert checks
    assert all(c.src_schema == "rbac" for c in checks)
    assert any(c.src_table == "subjects" and c.src_cols == ["user_id"] for c in checks)


def test_build_fk_checks_skips_columns_the_plan_adds():
    op = AddColumnOp(
        schema="rbac",
        table="subjects",
        column_name="user_id",
        column_def=RbacSubject.__table__.columns["user_id"],
    )
    checks = _build_fk_checks(["rbac"], MigrationPlan.build([op]))
    assert not any(c.src_table == "subjects" and c.src_cols == ["user_id"] for c in checks)
    assert any(c.src_table == "subjects" and c.src_cols == ["group_id"] for c in checks)


def test_build_orphan_delete_sql_single_column():
    c = _FkCheck("rbac", "subjects", "rbac", "users", ["user_id"], ["id"])
    sql = _build_orphan_delete_sql(c)
    assert sql.startswith("DELETE FROM rbac.subjects AS src")
    assert "src.user_id IS NOT NULL" in sql
    assert "NOT EXISTS (SELECT 1 FROM rbac.users AS ref WHERE src.user_id = ref.id)" in sql


def test_build_orphan_delete_sql_multiple_columns():
    c = _FkCheck("a", "t", "b", "u", ["x", "y"], ["i", "j"])
    sql = _build_orphan_delete_sql(c)
    assert "src.x IS NOT NULL OR src.y IS NOT NULL" in sql
    assert "src.x = ref.i" in sql
    assert "src.y = ref.j" in sql


def test_delete_orphans_converges_in_three_passes():
    runs = [
        MagicMock(stdout="DELETE 2", stderr=""),
        MagicMock(stdout="DELETE 1", stderr=""),
        MagicMock(stdout="DELETE 0", stderr=""),
    ]
    checks = [_FkCheck("rbac", "subjects", "rbac", "users", ["user_id"], ["id"])]
    with patch.object(dpr, "subprocess") as sp:
        sp.run.side_effect = runs
        _delete_orphans(checks, "postgresql://u:p@localhost/kronicle_unit_test")
    assert sp.run.call_count == 3


def test_delete_orphans_stops_when_no_delete_lines():
    checks = [_FkCheck("rbac", "subjects", "rbac", "users", ["user_id"], ["id"])]
    with patch.object(dpr, "subprocess") as sp:
        sp.run.return_value = _completed("UPDATE 1\n")
        _delete_orphans(checks, "dummy")
    assert sp.run.call_count == 1


def test_delete_orphans_non_convergent_caps_at_ten_passes():
    checks = [_FkCheck("rbac", "subjects", "rbac", "users", ["user_id"], ["id"])]
    with patch.object(dpr, "subprocess") as sp:
        sp.run.return_value = _completed("DELETE 1\n")
        _delete_orphans(checks, "dummy")
    assert sp.run.call_count == 10


def test_delete_orphans_runs_one_psql_per_check_per_pass():
    checks = [
        _FkCheck("rbac", "subjects", "rbac", "users", ["user_id"], ["id"]),
        _FkCheck("rbac", "subjects", "rbac", "groups", ["group_id"], ["id"]),
    ]
    with patch.object(dpr, "subprocess") as sp:
        sp.run.return_value = _completed("DELETE 0\n")
        _delete_orphans(checks, "dummy")
    assert sp.run.call_count == 2


def test_delete_orphans_ignores_malformed_delete_lines():
    checks = [_FkCheck("rbac", "subjects", "rbac", "users", ["user_id"], ["id"])]
    with patch.object(dpr, "subprocess") as sp:
        sp.run.return_value = _completed("DELETE nope\nDELETE\n")
        _delete_orphans(checks, "dummy")
    assert sp.run.call_count == 1


# ==================================================================================================
# Tracking-table synchronization
# ==================================================================================================


def test_sync_tracking_table_noop_when_no_columns_missing():
    conn = MagicMock()
    conn.dialect = postgresql.dialect()
    inspector = MagicMock()
    inspector.get_columns.return_value = [{"name": n} for n in CoreSchemaMigrationState.__table__.columns.keys()]
    with patch.object(dpr, "inspect", return_value=inspector):
        RbacSchemasProvisioner._sync_tracking_table(conn, CoreSchemaMigrationState)
    conn.execute.assert_not_called()


def test_sync_tracking_table_adds_missing_columns():
    conn = MagicMock()
    conn.dialect = postgresql.dialect()
    inspector = MagicMock()
    inspector.get_columns.return_value = [{"name": "id"}]
    with patch.object(dpr, "inspect", return_value=inspector):
        RbacSchemasProvisioner._sync_tracking_table(conn, CoreSchemaMigrationState)
    assert conn.execute.call_count == len(CoreSchemaMigrationState.__table__.columns.keys()) - 1
    first_sql = str(conn.execute.call_args_list[0].args[0])
    assert first_sql.startswith("ALTER TABLE core.schema_migration_state ADD COLUMN ")
    assert "NOT NULL" in first_sql


# ==================================================================================================
# Tracking-table existence checks
# ==================================================================================================


def test_check_tracking_tables_exist_true_when_both_present():
    p = _provisioner()
    with patch.object(dpr, "create_engine", return_value=_engine((1,))) as eng:
        assert p.check_tracking_tables_exist("core", "url") is True
    eng.assert_called_once()


def test_check_tracking_tables_exist_history_missing_returns_false():
    p = _provisioner()
    with patch.object(dpr, "create_engine", return_value=_engine_with_first([(1,), None])) as eng:
        assert p.check_tracking_tables_exist("core", "url") is False
    eng.assert_called_once()


def test_check_tracking_tables_exist_unknown_schema_returns_true():
    p = _provisioner()
    with patch.object(dpr, "create_engine") as eng:
        assert p.check_tracking_tables_exist("unknown_schema", "url") is True
    eng.assert_not_called()


# ==================================================================================================
# ensure_tracking_tables / _load_stored_state
# ==================================================================================================


def test_ensure_tracking_tables_creates_and_syncs_each_schema():
    p = _provisioner()
    with (
        patch.object(CoreSchemaMigrationState, "ensure_table") as core_ensure,
        patch.object(RbacSchemaMigrationState, "ensure_table") as rbac_ensure,
        patch.object(RbacSchemasProvisioner, "_sync_tracking_table") as sync,
    ):
        p.ensure_tracking_tables(MagicMock())
    assert core_ensure.call_count == 1
    assert rbac_ensure.call_count == 1
    assert sync.call_count == 4


def test_register_schema_models_is_idempotent():
    dpr._register_schema_models()
    assert dpr._SCHEMA_STATE["core"] is CoreSchemaMigrationState


def test_load_stored_state_returns_row():
    p = _provisioner()
    row = _StateRow()
    assert (
        p._load_stored_state(_engine(row).connect.return_value.__enter__.return_value, CoreSchemaMigrationState)
        is not None
    )


def test_load_stored_state_none_when_row_missing():
    p = _provisioner()
    engine = _engine(None)
    conn = engine.connect.return_value.__enter__.return_value
    assert p._load_stored_state(conn, CoreSchemaMigrationState) is None


def test_load_stored_state_none_when_execution_fails():
    p = _provisioner()
    engine = _engine(None)
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.side_effect = RuntimeError("no table")
    assert p._load_stored_state(conn, CoreSchemaMigrationState) is None


# ==================================================================================================
# pre_migration_check
# ==================================================================================================


def test_pre_migration_check_first_migration_records_no_previous_revision():
    p = _provisioner()
    with (
        patch.object(p, "_compute_db_hash", side_effect=["h_core", "h_rbac"]),
        patch.object(p, "_compute_metadata_hash", side_effect=["h_core", "h_rbac"]),
        patch.object(p, "_load_stored_state", return_value=None),
    ):
        p.pre_migration_check(MagicMock())
    assert p._previous_revisions == {"core": None, "rbac": None}


def test_pre_migration_check_records_previous_revisions_when_state_matches():
    p = _provisioner()
    rows = [_StateRow("r-core", "h_core"), _StateRow("r-rbac", "h_rbac")]
    with (
        patch.object(p, "_compute_db_hash", side_effect=["h_core", "h_rbac"]),
        patch.object(p, "_compute_metadata_hash", side_effect=["h_core", "h_rbac"]),
        patch.object(p, "_load_stored_state", side_effect=rows),
    ):
        p.pre_migration_check(MagicMock())
    assert p._previous_revisions == {"core": "r-core", "rbac": "r-rbac"}


def test_pre_migration_check_drift_is_false_positive_when_metadata_matches():
    p = _provisioner()
    rows = [_StateRow("r-core", "stored_core"), _StateRow("r-rbac", "stored_rbac")]
    with (
        patch.object(p, "_compute_db_hash", side_effect=["actual_core", "actual_rbac"]),
        patch.object(
            p, "_compute_metadata_hash", side_effect=["actual_core", "actual_core", "actual_rbac", "actual_rbac"]
        ),
        patch.object(p, "_load_stored_state", side_effect=rows),
        patch.object(dpr, "DatabaseCatalogBuilder") as dcb,
    ):
        p.pre_migration_check(MagicMock())
    assert p._previous_revisions == {"core": "r-core", "rbac": "r-rbac"}
    dcb.assert_not_called()


def test_pre_migration_check_drills_down_on_real_structural_difference():
    p = _provisioner()
    rows = [_StateRow("r-core", "h_core"), _StateRow("r-rbac", "stored_rbac")]
    with (
        patch.object(p, "_compute_db_hash", side_effect=["h_core", "actual_rbac"]),
        patch.object(p, "_compute_metadata_hash", side_effect=["h_core", "meta_rbac", "meta_rbac"]),
        patch.object(p, "_load_stored_state", side_effect=rows),
        patch.object(dpr, "DatabaseCatalogBuilder") as dcb,
    ):
        catalog = MagicMock()
        dcb.return_value.from_database.return_value = catalog
        dcb.from_metadata.return_value = catalog
        p.pre_migration_check(MagicMock())
    dcb.return_value.from_database.assert_called_once()
    assert dcb.from_metadata.call_count == 1


def test_pre_migration_check_logs_identical_catalogs_when_catalog_matches():
    p = _provisioner()
    rows = [_StateRow("r-core", "h_core"), _StateRow("r-rbac", "stored_rbac")]
    with (
        patch.object(p, "_compute_db_hash", side_effect=["h_core", "actual_rbac"]),
        patch.object(p, "_compute_metadata_hash", side_effect=["h_core", "meta_rbac", "meta_rbac"]),
        patch.object(p, "_load_stored_state", side_effect=rows),
        patch.object(dpr, "DatabaseCatalogBuilder") as dcb,
    ):
        db_cat = MagicMock()
        meta_cat = MagicMock()
        db_cat.as_tuple.return_value = (1, 2, 3)
        meta_cat.as_tuple.return_value = (1, 2, 3, 4, 5, 6, 7)
        dcb.return_value.from_database.return_value = db_cat
        dcb.from_metadata.return_value = meta_cat
        p.pre_migration_check(MagicMock())


# ==================================================================================================
# Hashing helpers
# ==================================================================================================


def test_compute_db_hash_delegates_to_builder():
    p = _provisioner()
    with patch.object(dpr, "DatabaseCatalogBuilder") as dcb:
        dcb.return_value.from_database.return_value.compute_hash.return_value = "abc123"
        assert p._compute_db_hash(MagicMock(), "core") == "abc123"
    dcb.return_value.from_database.assert_called_once_with("core")


def test_compute_metadata_hash_delegates_to_builder():
    p = _provisioner()
    with patch.object(dpr, "DatabaseCatalogBuilder") as dcb:
        dcb.from_metadata.return_value.compute_hash.return_value = "def456"
        assert p._compute_metadata_hash("core") == "def456"
    dcb.from_metadata.assert_called_once()


# ==================================================================================================
# build_plan / analyze
# ==================================================================================================


def test_build_plan_builds_from_proposal_operations():
    p = _provisioner()
    p.rbac_db._engine = _engine(None)
    proposal = MagicMock()
    with patch.object(dpr, "MigrationProposal", return_value=proposal) as mp:
        proposal.to_operations.return_value = [_add_col_op()]
        plan = p.build_plan()
    assert isinstance(plan, MigrationPlan)
    assert len(plan.operations) == 1
    mp.assert_called_once()


def test_apply_plan_without_connection_uses_engine_begin():
    p = _provisioner()
    plan = MigrationPlan.build([_add_col_op()])
    p.rbac_db._engine = _exec_engine()
    with patch.object(dpr, "MigrationContext") as mc, patch.object(dpr, "Operations") as ops:
        p.apply_plan(plan)
    mc.configure.assert_called_once()
    ops.assert_called_once()
    assert p.rbac_db._engine.begin.call_count == 1


def test_apply_plan_with_connection_uses_provided_connection():
    p = _provisioner()
    p.rbac_db._engine = _exec_engine()
    plan = MigrationPlan.build([_add_col_op()])
    connection = MagicMock()
    with patch.object(dpr, "MigrationContext") as mc, patch.object(dpr, "Operations") as ops:
        p.apply_plan(plan, connection=connection)
    mc.configure.assert_called_once_with(connection)
    ops.assert_called_once()
    assert p.rbac_db._engine.begin.call_count == 0


def test_analyze_no_work_refreshes_state():
    p = _provisioner()
    p.rbac_db._engine = _engine(None)
    with (
        patch.object(p, "check_analysis_requirements"),
        patch.object(p, "check_tracking_prerequisites", return_value=[]),
        patch.object(p, "pre_migration_check"),
        patch.object(p, "build_plan", return_value=MigrationPlan.build([])),
        patch.object(p, "refresh_state_if_needed") as refresh,
    ):
        p.analyze()
    assert p._has_work is False
    assert p._auto_approve is False
    refresh.assert_called_once()


def test_analyze_has_work_when_tracking_fixes_needed():
    p = _provisioner()
    p.rbac_db._engine = _engine(None)
    with (
        patch.object(p, "check_analysis_requirements"),
        patch.object(p, "check_tracking_prerequisites", return_value=["core"]),
        patch.object(p, "pre_migration_check"),
        patch.object(p, "build_plan", return_value=MigrationPlan.build([])),
        patch.object(p, "refresh_state_if_needed") as refresh,
    ):
        p.analyze(auto_approve=True)
    assert p._has_work is True
    assert p._auto_approve is True
    refresh.assert_not_called()


# ==================================================================================================
# ask_validation
# ==================================================================================================


def test_ask_validation_auto_approve_skips_prompt_even_for_destructive():
    p = _provisioner()
    p._auto_approve = True
    p._plan = MigrationPlan.build([_drop_idx_op()])
    p._tracking_fixes = []
    with patch.object(p, "check_mutation_requirements") as cmr, patch.object(p, "_confirm") as confirm:
        assert p.ask_validation(auto_approve_if_non_destructive=True) is True
    cmr.assert_called_once()
    confirm.assert_not_called()


def test_ask_validation_prompts_when_destructive_auto_approve_requested():
    p = _provisioner()
    p._auto_approve = False
    p._plan = MigrationPlan.build([_drop_idx_op()])
    p._tracking_fixes = []
    with patch.object(p, "check_mutation_requirements"), patch.object(p, "_confirm", return_value=False) as confirm:
        assert p.ask_validation(auto_approve_if_non_destructive=True) is False
    confirm.assert_called_once()


def test_ask_validation_auto_approves_non_destructive_plan():
    p = _provisioner()
    p._auto_approve = False
    p._plan = MigrationPlan.build([_add_col_op()])
    p._tracking_fixes = []
    with patch.object(p, "check_mutation_requirements"), patch.object(p, "_confirm") as confirm:
        assert p.ask_validation(auto_approve_if_non_destructive=True) is True
    confirm.assert_not_called()


def test_ask_validation_confirms_when_prompted():
    p = _provisioner()
    p._auto_approve = False
    p._plan = MigrationPlan.build([_add_col_op()])
    p._tracking_fixes = ["core"]
    with patch.object(p, "check_mutation_requirements"), patch.object(p, "_confirm", return_value=True) as confirm:
        assert p.ask_validation(auto_approve_if_non_destructive=False) is True
    confirm.assert_called_once()


# ==================================================================================================
# backup / restore_backup / _table_exists / clean_orphans
# ==================================================================================================


def test_backup_runs_pg_dump_for_each_schema():
    p = _provisioner()
    backup_file = Path("/tmp/kronicle_rbac/rbac.dump")
    with (
        patch.dict(os.environ, {KRONICLE_RBAC_BACKUP: "/tmp/kronicle_rbac"}),
        patch.object(dpr, "backup_path", return_value=backup_file),
        patch.object(dpr, "subprocess") as sp,
    ):
        sp.run.return_value = _completed()
        assert p.backup() == backup_file
    cmd = sp.run.call_args.args[0]
    assert cmd[0] == "pg_dump"
    assert "-Fc" in cmd and "-f" in cmd and str(backup_file) in cmd
    for schema in p.schemas:
        assert schema in cmd


def test_backup_failure_raises_runtime_error():
    p = _provisioner()
    with (
        patch.dict(os.environ, {KRONICLE_RBAC_BACKUP: "/tmp/kronicle_rbac"}),
        patch.object(dpr, "backup_path", return_value=Path("/tmp/kronicle_rbac/rbac.dump")),
        patch(
            "kronicle.db.migration.orchestrators.db_rbac_provisioner.subprocess.run",
            side_effect=CalledProcessError(2, "pg_dump", stderr="disk full"),
        ),
    ):
        with pytest.raises(RuntimeError, match="Backup failed"):
            p.backup()


def test_restore_backup_skips_when_no_backup_file():
    p = _provisioner()
    with (
        patch.object(dpr, "subprocess") as sp,
        patch.object(p, "record_migration_state") as record,
    ):
        p.restore_backup(None)
    sp.run.assert_not_called()
    record.assert_not_called()


def test_restore_backup_runs_pg_restore_and_records_failure():
    p = _provisioner()
    backup_file = Path("/tmp/kronicle_rbac/rbac.dump")
    p._plan = MigrationPlan.build([_add_col_op()])
    p.rbac_db._engine = _exec_engine()
    with (
        patch.object(dpr, "subprocess") as sp,
        patch.object(dpr, "create_engine", return_value=_exec_engine()),
        patch.object(p, "ensure_tracking_tables"),
        patch.object(p, "record_migration_state") as record,
    ):
        p.restore_backup(backup_file)
    cmd = sp.run.call_args.args[0]
    assert cmd[0] == "pg_restore" and "--clean" in cmd and str(backup_file) in cmd
    record.assert_called_once_with(p._plan, success=False)


def test_restore_backup_drops_tracking_tables_before_pg_restore():
    """Tracking tables (absent from the dump) must be dropped before pg_restore --clean."""
    from sqlalchemy.schema import Table

    p = _provisioner()
    backup_file = Path("/tmp/kronicle_rbac/rbac.dump")
    p._plan = MigrationPlan.build([_add_col_op()])
    p.rbac_db._engine = _exec_engine()

    engine = _exec_engine()
    conn = engine.begin.return_value.__enter__.return_value
    dropped = []
    orig_drop = Table.drop
    Table.drop = lambda self, bind=None, checkfirst=True, **kw: dropped.append(bind)

    try:
        with (
            patch.object(dpr, "subprocess") as sp,
            patch.object(dpr, "create_engine", return_value=engine),
            patch.object(p, "ensure_tracking_tables"),
            patch.object(p, "record_migration_state"),
        ):
            p.restore_backup(backup_file)
    finally:
        Table.drop = orig_drop

    # state+history × core+rbac = four Table.drop() calls, all on the restore connection
    assert len(dropped) == 4
    assert all(d is conn for d in dropped)
    assert sp.run.call_args.args[0][0] == "pg_restore"


def test_clean_orphans_noop_when_no_fk_checks():
    p = _provisioner()
    plan = MigrationPlan.build([])
    with (
        patch.object(dpr, "_build_fk_checks", return_value=[]),
        patch.object(dpr, "_delete_orphans") as dop,
    ):
        p.clean_orphans(plan)
    dop.assert_not_called()


def test_clean_orphans_skips_fk_when_referencing_table_missing():
    p = _provisioner()
    plan = MigrationPlan.build([])
    present = _FkCheck("rbac", "subjects", "rbac", "users", ["user_id"], ["id"])
    missing = _FkCheck("rbac", "rbac_events", "rbac", "users", ["actor_id"], ["id"])
    with (
        patch.object(dpr, "_build_fk_checks", return_value=[present, missing]),
        patch.object(p, "_table_exists", side_effect=[True, False]),
        patch.object(dpr, "_delete_orphans") as dop,
    ):
        p.clean_orphans(plan)
    dop.assert_called_once()
    assert len(dop.call_args.args[0]) == 1
    assert dop.call_args.args[0][0].src_table == "subjects"


def test_clean_orphans_deletes_orphans_for_existing_tables():
    p = _provisioner()
    plan = MigrationPlan.build([])
    checks = [_FkCheck("rbac", "subjects", "rbac", "users", ["user_id"], ["id"])]
    with (
        patch.object(dpr, "_build_fk_checks", return_value=checks),
        patch.object(p, "_table_exists", return_value=True),
        patch.object(dpr, "_delete_orphans") as dop,
    ):
        p.clean_orphans(plan)
    dop.assert_called_once()


def test_table_exists_true():
    p = _provisioner()
    with patch.object(dpr, "create_engine", return_value=_engine((1,))):
        assert p._table_exists("rbac", "subjects", "url") is True


def test_table_exists_false():
    p = _provisioner()
    with patch.object(dpr, "create_engine", return_value=_engine(None)):
        assert p._table_exists("rbac", "subjects", "url") is False


# ==================================================================================================
# Connectivity + requirement checks
# ==================================================================================================


def test_can_connect_success():
    p = _provisioner()
    with patch.object(dpr, "create_engine", return_value=_engine((1,))) as eng:
        assert p._can_connect("postgresql://u:p@localhost:5432/kronicle_unit_test", "rbac") is True
    eng.assert_called_once()


def test_can_connect_failure_logs_and_returns_false():
    p = _provisioner()
    with patch.object(dpr, "create_engine", side_effect=RuntimeError("boom")):
        assert p._can_connect("postgresql://u:p@localhost:5432/kronicle_unit_test", "rbac") is False


def test_check_analysis_requirements_passes():
    p = _provisioner()
    p.rbac_db._engine = _engine((1,))
    p.check_analysis_requirements()


def test_check_analysis_requirements_raises_on_error():
    p = _provisioner()
    engine = _engine(None)
    engine.connect.return_value.__enter__.return_value.execute.side_effect = RuntimeError("denied")
    p.rbac_db._engine = engine
    with pytest.raises(RuntimeError, match="Cannot read schemas"):
        p.check_analysis_requirements()


def test_check_mutation_requirements_wraps_backup_write_check():
    p = _provisioner()
    with patch.object(p, "check_backup_writable") as cbw:
        p.check_mutation_requirements()
    cbw.assert_called_once()


def test_check_backup_writable_ok():
    p = _provisioner()
    with (
        patch.dict(os.environ, {KRONICLE_RBAC_BACKUP: "/tmp/kronicle_rbac"}),
        patch.object(os, "access", return_value=True),
    ):
        p.check_backup_writable()


def test_check_backup_writable_not_writable_raises():
    p = _provisioner()
    with (
        patch.dict(os.environ, {KRONICLE_RBAC_BACKUP: "/tmp/kronicle_rbac"}),
        patch.object(os, "access", return_value=False),
    ):
        with pytest.raises(RuntimeError, match="not writable"):
            p.check_backup_writable()


def test_check_tracking_prerequisites_reports_missing_schemas():
    p = _provisioner()
    with patch.object(p, "check_tracking_tables_exist", side_effect=[False, True]) as ctte:
        assert p.check_tracking_prerequisites() == ["core"]
    ctte.assert_has_calls([call("core", p.rbac_url), call("rbac", p.rbac_url)])


# ==================================================================================================
# execute_plan
# ==================================================================================================


def test_execute_plan_safe_plan_records_and_sets_metadata():
    p = _provisioner()
    plan = MigrationPlan.build([_add_col_op()])
    p._plan = plan
    p.rbac_db._engine = _exec_engine()
    with (
        patch.object(dpr, "create_engine", return_value=_exec_engine()),
        patch.object(p, "ensure_tracking_tables") as ensure,
        patch.object(p, "clean_orphans") as clean,
        patch.object(p, "apply_plan") as apply_plan,
        patch.object(p, "record_migration_state") as record,
    ):
        p.execute_plan()
    ensure.assert_not_called()
    clean.assert_called_once()
    assert apply_plan.call_args.args[0] is plan
    assert "connection" in apply_plan.call_args.kwargs
    record.assert_called_once_with(plan, success=True)
    assert p._safety == SafetyLevel.SAFE
    assert p._revision == plan.revision
    assert p._applied_ops == 1


def test_execute_plan_sets_destructive_safety():
    p = _provisioner()
    plan = MigrationPlan.build([_drop_idx_op()])
    p._plan = plan
    p.rbac_db._engine = _exec_engine()
    with (
        patch.object(dpr, "create_engine", return_value=_exec_engine()),
        patch.object(p, "ensure_tracking_tables"),
        patch.object(p, "clean_orphans"),
        patch.object(p, "apply_plan"),
        patch.object(p, "record_migration_state"),
    ):
        p.execute_plan()
    assert p._safety == SafetyLevel.DESTRUCTIVE


def test_execute_plan_sets_warning_safety():
    p = _provisioner()
    plan = MigrationPlan.build([_add_unique_op()])
    p._plan = plan
    p.rbac_db._engine = _exec_engine()
    with (
        patch.object(dpr, "create_engine", return_value=_exec_engine()),
        patch.object(p, "ensure_tracking_tables"),
        patch.object(p, "clean_orphans"),
        patch.object(p, "apply_plan"),
        patch.object(p, "record_migration_state"),
    ):
        p.execute_plan()
    assert p._safety == SafetyLevel.WARNING


def test_execute_plan_creates_tracking_tables_when_fixes_needed():
    p = _provisioner()
    plan = MigrationPlan.build([_add_col_op()])
    p._plan = plan
    p._tracking_fixes = ["core"]
    p.rbac_db._engine = _exec_engine()
    with (
        patch.object(dpr, "create_engine", return_value=_exec_engine()),
        patch.object(p, "ensure_tracking_tables") as ensure,
        patch.object(p, "clean_orphans"),
        patch.object(p, "apply_plan"),
        patch.object(p, "record_migration_state"),
    ):
        p.execute_plan()
    ensure.assert_called_once()


# ==================================================================================================
# record_migration_state / refresh_state_if_needed
# ==================================================================================================


def test_record_migration_state_writes_history_and_state_per_schema():
    p = _provisioner()
    plan = MigrationPlan.build([_add_col_op()])
    p.rbac_db._engine = _exec_engine()
    with (
        patch.object(p, "ensure_tracking_tables") as ensure,
        patch.object(p, "_compute_db_hash", side_effect=["h_core", "h_rbac"]),
        patch.object(dpr, "DatabaseCatalogBuilder") as dcb,
    ):
        dcb.from_metadata.return_value.tables = []
        p.record_migration_state(plan, success=True)
    ensure.assert_called_once()
    assert p.rbac_db._engine.begin.return_value.__enter__.return_value.execute.call_count == 4


def test_record_migration_state_records_with_ops():
    p = _provisioner()
    plan = MigrationPlan.build([_add_col_op()])
    p.rbac_db._engine = _exec_engine()
    with (
        patch.object(p, "ensure_tracking_tables"),
        patch.object(p, "_compute_db_hash", side_effect=["h_core", "h_rbac"]),
        patch.object(dpr, "DatabaseCatalogBuilder"),
    ):
        p.record_migration_state(plan, success=False)


def test_refresh_state_if_needed_inserts_when_hash_drifted():
    p = _provisioner()
    plan = MigrationPlan.build([])
    p.rbac_db._engine = _exec_engine()
    conn = p.rbac_db._engine.begin.return_value.__enter__.return_value
    row = MagicMock()
    row.schema_hash = "old"
    conn.execute.return_value.first.side_effect = [None, row]
    with patch.object(p, "_compute_db_hash", side_effect=["ignored", "new"]):
        p.refresh_state_if_needed(plan)
    assert conn.execute.call_count == 3


def test_refresh_state_if_needed_skips_when_hash_unchanged():
    p = _provisioner()
    plan = MigrationPlan.build([])
    p.rbac_db._engine = _exec_engine()
    conn = p.rbac_db._engine.begin.return_value.__enter__.return_value
    row = MagicMock()
    row.schema_hash = "same"
    conn.execute.return_value.first.side_effect = [None, row]
    with patch.object(p, "_compute_db_hash", side_effect=["same"]):
        p.refresh_state_if_needed(plan)
    assert conn.execute.call_count == 2


# ==================================================================================================
# run_post_analysis / _is_non_destructive
# ==================================================================================================


def test_is_non_destructive():
    p = _provisioner()
    assert p._is_non_destructive(MigrationPlan.build([_add_col_op()])) is True
    assert p._is_non_destructive(MigrationPlan.build([_drop_idx_op()])) is False


def test_run_post_analysis_true_when_converged():
    p = _provisioner()
    with patch.object(p, "build_plan", return_value=MigrationPlan.build([])):
        assert p.run_post_analysis() is True


def test_run_post_analysis_false_when_operations_outstanding():
    p = _provisioner()
    with patch.object(p, "build_plan", return_value=MigrationPlan.build([_add_col_op()])):
        assert p.run_post_analysis() is False
