# tests/unit/db/migration/orchestrators/test_db_data_provisioner.py
"""Unit tests for kronicle.db.migration.orchestrators.db_data_provisioner."""

import os
from pathlib import Path
from subprocess import CalledProcessError
from unittest.mock import MagicMock, patch

import pytest

from kronicle.db.data.models._registry import DATA_NAMESPACE
from kronicle.db.migration.engine.operations import SafetyLevel
from kronicle.db.migration.orchestrators import db_data_provisioner as ddp
from kronicle.db.migration.orchestrators.db_data_provisioner import (
    SYSTEM_COLUMNS,
    TARGET_PK_COLUMNS,
    ChannelDrift,
    DataSchemaProvisioner,
)
from kronicle.deps.settings_env import KRONICLE_DATA_BACKUP

from .conftest import make_db_settings

# ==================================================================================================
# Helpers
# ==================================================================================================


def _completed(stdout: str = ""):
    return MagicMock(stdout=stdout, stderr="")


def _provisioner(**settings_kwargs):
    return DataSchemaProvisioner(db_settings=make_db_settings(**settings_kwargs))


def _drift(
    table="channel_x",
    missing: list[str] | None = None,
    pk: list[str] | None = None,
    hyper: bool = True,
):
    return ChannelDrift(
        table=table,
        missing_columns=missing or [],
        pk_columns=pk if pk is not None else list(TARGET_PK_COLUMNS),
        is_hypertable=hyper,
    )


# ==================================================================================================
# _psql / _require_tables
# ==================================================================================================


def test_psql_runs_psql_and_strips_output():
    p = _provisioner()
    with patch.object(ddp, "subprocess") as sp:
        sp.run.return_value = _completed(" 1\n\n")
        assert p._psql("SELECT 1", tuples=True) == "1"
    cmd = sp.run.call_args.args[0]
    assert cmd[0] == "psql"
    assert cmd[2] == p.data_url
    assert cmd[cmd.index("-c") + 1] == "SELECT 1"
    assert "-t" in cmd and "-A" in cmd


def test_psql_defaults_to_data_url_without_tuple_flag():
    p = _provisioner()
    with patch.object(ddp, "subprocess") as sp:
        sp.run.return_value = _completed("ok")
        p._psql("SELECT 1")
    cmd = sp.run.call_args.args[0]
    assert "-t" not in cmd and "-A" not in cmd
    assert cmd[2] == p.data_url


def test_properties_expose_chan_credentials_and_url():
    settings = make_db_settings()
    p = _provisioner()
    assert p.chan_user == settings._chan_usr
    assert p.chan_url == settings.channel_connection_url == p.data_url
    assert p._backup_connection_url() == p.data_url


def test_require_tables_true_when_query_succeeds():
    p = _provisioner()
    with patch.object(p, "_psql", return_value="1"):
        assert p._require_tables() is True


def test_require_tables_false_when_psql_fails():
    p = _provisioner()
    with patch.object(p, "_psql", side_effect=CalledProcessError(1, "psql")):
        assert p._require_tables() is False


# ==================================================================================================
# Read-only catalog helpers
# ==================================================================================================


def test_list_channel_tables_filters_empty_lines():
    p = _provisioner()
    out = "channel_a" + "1" * 31 + "\nchannel_b" + "2" * 31 + "\n\n"
    with patch.object(p, "_psql", return_value=out):
        assert p.list_channel_tables() == ["channel_a" + "1" * 31, "channel_b" + "2" * 31]


def test_list_channel_tables_parses_single_table():
    p = _provisioner()
    with patch.object(p, "_psql", return_value="channel_c" + "c" * 32 + "\n"):
        assert p.list_channel_tables() == ["channel_c" + "c" * 32]


def test_list_channel_tables_empty():
    p = _provisioner()
    with patch.object(p, "_psql", return_value=""):
        assert p.list_channel_tables() == []


def test_table_columns_parses_name_and_type():
    p = _provisioner()
    out = "time\ttimestamp with time zone\nrow_id\tbigint\n\n"
    with patch.object(p, "_psql", return_value=out):
        assert p._table_columns("channel_x") == {"time": "timestamp with time zone", "row_id": "bigint"}


def test_table_columns_skips_lines_without_tab():
    p = _provisioner()
    out = "time\ttimestamp with time zone\ncorrupt_line\nrow_id\tbigint\n"
    with patch.object(p, "_psql", return_value=out):
        assert p._table_columns("channel_x") == {"time": "timestamp with time zone", "row_id": "bigint"}


def test_primary_key_columns_returns_ordered_names():
    p = _provisioner()
    with patch.object(p, "_psql", return_value="row_id\ntime\n"):
        assert p._primary_key_columns("channel_x") == ["row_id", "time"]


def test_primary_key_columns_empty():
    p = _provisioner()
    with patch.object(p, "_psql", return_value=""):
        assert p._primary_key_columns("channel_x") == []


def test_primary_key_name_returns_conname():
    p = _provisioner()
    with patch.object(p, "_psql", return_value="channel_x_pkey"):
        assert p._primary_key_name("channel_x") == "channel_x_pkey"


def test_primary_key_name_none_when_no_pk():
    p = _provisioner()
    with patch.object(p, "_psql", return_value=""):
        assert p._primary_key_name("channel_x") is None


def test_is_hypertable_true():
    p = _provisioner()
    with patch.object(p, "_psql", return_value="1"):
        assert p._is_hypertable("channel_x") is True


def test_is_hypertable_false():
    p = _provisioner()
    with patch.object(p, "_psql", return_value=""):
        assert p._is_hypertable("channel_x") is False


# ==================================================================================================
# Existence checks
# ==================================================================================================


def test_check_tracking_tables_exist_true_when_both_present():
    p = _provisioner()
    out = "schema_migration_state\nschema_migration_history\n"
    with patch.object(p, "_psql", return_value=out):
        assert p.check_tracking_tables_exist() is True


def test_check_tracking_tables_exist_false_when_one_missing():
    p = _provisioner()
    out = "schema_migration_state\n"
    with patch.object(p, "_psql", return_value=out):
        assert p.check_tracking_tables_exist() is False


def test_check_tracking_tables_exist_false_when_empty():
    p = _provisioner()
    with patch.object(p, "_psql", return_value=""):
        assert p.check_tracking_tables_exist() is False


def test_check_channel_metadata_table_exists_true():
    p = _provisioner()
    with patch.object(p, "_psql", return_value="1"):
        assert p.check_channel_metadata_table_exists() is True


def test_check_channel_metadata_table_exists_false():
    p = _provisioner()
    with patch.object(p, "_psql", return_value=""):
        assert p.check_channel_metadata_table_exists() is False


# ==================================================================================================
# analyze
# ==================================================================================================


def test_analyze_no_work_when_converged():
    p = _provisioner()
    with (
        patch.object(p, "check_tracking_tables_exist", return_value=True),
        patch.object(p, "check_channel_metadata_table_exists", return_value=True),
        patch.object(p, "list_channel_tables", return_value=[]),
    ):
        p.analyze()
    assert p._has_work is False
    assert p._tracking_missing is False
    assert p._metadata_missing is False
    assert p.channels == []


def test_analyze_reports_tracking_and_metadata_gaps():
    p = _provisioner()
    with (
        patch.object(p, "check_tracking_tables_exist", return_value=False),
        patch.object(p, "check_channel_metadata_table_exists", return_value=False),
        patch.object(p, "list_channel_tables", return_value=[]),
    ):
        p.analyze()
    assert p._has_work is True
    assert p._tracking_missing is True
    assert p._metadata_missing is True


def test_analyze_detects_channel_drift():
    p = _provisioner()
    with (
        patch.object(p, "check_tracking_tables_exist", return_value=True),
        patch.object(p, "check_channel_metadata_table_exists", return_value=True),
        patch.object(p, "list_channel_tables", return_value=["channel_x"]),
        patch.object(p, "_table_columns", return_value={"time": "timestamp with time zone"}),
        patch.object(p, "_primary_key_columns", return_value=["time"]),
        patch.object(p, "_is_hypertable", return_value=True),
    ):
        p.analyze()
    assert p._has_work is True
    assert len(p.channels) == 1
    drift = p.channels[0]
    assert drift.table == "channel_x"
    assert sorted(drift.missing_columns) == ["received_at", "row_id"]
    assert drift.pk_columns == ["time"]
    assert drift.is_hypertable is True


def test_analyze_channel_in_compliance_has_no_work():
    p = _provisioner()
    with (
        patch.object(p, "check_tracking_tables_exist", return_value=True),
        patch.object(p, "check_channel_metadata_table_exists", return_value=True),
        patch.object(p, "list_channel_tables", return_value=["channel_x"]),
        patch.object(p, "_table_columns", return_value=dict.fromkeys(SYSTEM_COLUMNS, "t")),
        patch.object(p, "_primary_key_columns", return_value=list(TARGET_PK_COLUMNS)),
        patch.object(p, "_is_hypertable", return_value=True),
    ):
        p.analyze()
    assert p._has_work is False


# ==================================================================================================
# ask_validation
# ==================================================================================================


def test_ask_validation_auto_approve_skips_prompt():
    p = _provisioner()
    p._tracking_missing = True
    p._metadata_missing = True
    p.channels = []
    with patch.object(p, "_confirm") as confirm:
        assert p.ask_validation(auto_approve=True) is True
    confirm.assert_not_called()


def test_ask_validation_prompts_when_not_auto_approved():
    p = _provisioner()
    p._tracking_missing = False
    p._metadata_missing = False
    p.channels = []
    with patch.object(p, "_confirm", return_value=True) as confirm:
        assert p.ask_validation(auto_approve=False) is True
    confirm.assert_called_once()


def test_ask_validation_returns_false_when_user_declines():
    p = _provisioner()
    p._tracking_missing = True
    p._metadata_missing = True
    p.channels = []
    with patch.object(p, "_confirm", return_value=False) as confirm:
        assert p.ask_validation(auto_approve=False) is False
    confirm.assert_called_once()


def test_ask_validation_prompts_even_for_non_destructive_approval_flag():
    p = _provisioner()
    p._tracking_missing = False
    p._metadata_missing = False
    p.channels = [_drift(missing=["received_at"])]
    with patch.object(p, "_confirm", return_value=True) as confirm:
        assert p.ask_validation(auto_approve=False, auto_approve_if_non_destructive=True) is True
    confirm.assert_called_once()


def test_ask_validation_lists_pk_and_hypertable_only_issues():
    p = _provisioner()
    p._tracking_missing = False
    p._metadata_missing = False
    p.channels = [_drift(pk=["time"], hyper=False)]
    with patch.object(p, "_confirm", return_value=True):
        assert p.ask_validation(auto_approve=False) is True


# ==================================================================================================
# backup / restore_backup
# ==================================================================================================


def test_backup_runs_pg_dump_for_data_schema():
    p = _provisioner()
    backup_file = Path("/tmp/kronicle_data/data.dump")
    with (
        patch.dict(os.environ, {KRONICLE_DATA_BACKUP: "/tmp/kronicle_data"}),
        patch.object(ddp, "backup_path", return_value=backup_file),
        patch.object(ddp, "subprocess") as sp,
    ):
        sp.run.return_value = _completed()
        assert p.backup() == backup_file
    cmd = sp.run.call_args.args[0]
    assert cmd[0] == "pg_dump"
    assert "-Fc" in cmd and "-f" in cmd and str(backup_file) in cmd
    assert "-n" in cmd and DATA_NAMESPACE in cmd


def test_backup_failure_raises_runtime_error():
    p = _provisioner()
    with (
        patch.dict(os.environ, {KRONICLE_DATA_BACKUP: "/tmp/kronicle_data"}),
        patch.object(ddp, "backup_path", return_value=Path("/tmp/kronicle_data/data.dump")),
        patch(
            "kronicle.db.migration.orchestrators.db_data_provisioner.subprocess.run",
            side_effect=CalledProcessError(2, "pg_dump", stderr="disk full"),
        ),
    ):
        with pytest.raises(RuntimeError, match="Backup failed"):
            p.backup()


def test_restore_backup_skips_when_no_backup_file():
    p = _provisioner()
    with patch.object(ddp, "subprocess") as sp:
        p.restore_backup(None)
    sp.run.assert_not_called()


def test_restore_backup_runs_pg_restore():
    p = _provisioner()
    backup_file = Path("/tmp/kronicle_data/data.dump")
    with patch.object(ddp, "subprocess") as sp:
        p.restore_backup(backup_file)
    cmd = sp.run.call_args.args[0]
    assert cmd[0] == "pg_restore" and "--clean" in cmd and str(backup_file) in cmd
    assert sp.run.call_args.args[0][cmd.index("-d") + 1] == p.data_url


# ==================================================================================================
# execute_plan / run_post_analysis
# ==================================================================================================


def test_execute_plan_applies_tracking_metadata_and_transforms():
    p = _provisioner()
    p._tracking_missing = True
    p._metadata_missing = True
    p.channels = [_drift(table="channel_a"), _drift(table="channel_b")]
    with (
        patch.object(p, "_ensure_tracking_tables") as ensure,
        patch.object(p, "_psql") as psql,
        patch.object(p, "_transform_channel", side_effect=lambda _: True) as transform,
    ):
        p.execute_plan()
    ensure.assert_called_once()
    create_sql = str(psql.call_args.args[0])
    assert "CREATE TABLE" in create_sql
    assert transform.call_count == 2
    assert p._applied_ops == 4
    assert p._safety == SafetyLevel.DESTRUCTIVE


def test_execute_plan_safe_when_nothing_applied():
    p = _provisioner()
    p._tracking_missing = False
    p._metadata_missing = False
    p.channels = []
    with (
        patch.object(p, "_ensure_tracking_tables") as ensure,
        patch.object(p, "_psql") as psql,
        patch.object(p, "_transform_channel", side_effect=lambda _: False),
    ):
        p.execute_plan()
    ensure.assert_not_called()
    psql.assert_not_called()
    assert p._applied_ops == 0
    assert p._safety == SafetyLevel.SAFE


def test_run_post_analysis_true_when_converged():
    p = _provisioner()
    with patch.object(p, "analyze") as analyze:
        analyze.side_effect = lambda: setattr(p, "_has_work", False)
        assert p.run_post_analysis() is True


def test_run_post_analysis_false_when_work_outstanding():
    p = _provisioner()
    with patch.object(p, "analyze") as analyze:
        analyze.side_effect = lambda: setattr(p, "_has_work", True)
        assert p.run_post_analysis() is False


# ==================================================================================================
# Mutating helpers
# ==================================================================================================


def test_ensure_tracking_tables_creates_state_and_history():
    p = _provisioner()
    with patch.object(p, "_psql") as psql:
        p._ensure_tracking_tables()
    assert psql.call_count == 2
    state_sql = str(psql.call_args_list[0].args[0])
    history_sql = str(psql.call_args_list[1].args[0])
    assert f"CREATE TABLE IF NOT EXISTS {DATA_NAMESPACE}.schema_migration_state" in state_sql
    assert f"CREATE TABLE IF NOT EXISTS {DATA_NAMESPACE}.schema_migration_history" in history_sql


def test_transform_channel_noop_when_already_hypertable():
    p = _provisioner()
    with patch.object(p, "_psql") as psql:
        assert p._transform_channel(_drift()) is False
    psql.assert_not_called()


def test_transform_channel_drops_and_readds_primary_key():
    p = _provisioner()
    drift = _drift(pk=["time"], hyper=True)
    with (
        patch.object(p, "_primary_key_name", return_value="channel_x_pkey"),
        patch.object(p, "_psql") as psql,
    ):
        assert p._transform_channel(drift) is True
    sqls = [str(c.args[0]) for c in psql.call_args_list]
    assert f"ALTER TABLE {DATA_NAMESPACE}.channel_x DROP CONSTRAINT channel_x_pkey" in sqls
    assert f"ALTER TABLE {DATA_NAMESPACE}.channel_x ADD PRIMARY KEY (time, row_id)" in sqls


def test_transform_channel_adds_pk_when_no_existing_constraint():
    p = _provisioner()
    drift = _drift(pk=["time"], hyper=True)
    with (
        patch.object(p, "_primary_key_name", return_value=None),
        patch.object(p, "_psql") as psql,
    ):
        assert p._transform_channel(drift) is True
    assert psql.call_count == 1
    assert "ADD PRIMARY KEY (time, row_id)" in str(psql.call_args.args[0])


def test_transform_channel_creates_hypertable_when_missing():
    p = _provisioner()
    drift = _drift(hyper=False)
    with patch.object(p, "_psql") as psql:
        assert p._transform_channel(drift) is True
    sql = str(psql.call_args.args[0])
    assert f"create_hypertable('{DATA_NAMESPACE}.channel_x', 'time'" in sql
    assert "create_default_indexes => TRUE" in sql


def test_transform_channel_full_reconcile():
    p = _provisioner()
    drift = _drift(missing=["received_at"], pk=["time"], hyper=False)
    with (
        patch.object(p, "_primary_key_name", return_value="channel_x_pkey"),
        patch.object(p, "_psql") as psql,
    ):
        assert p._transform_channel(drift) is True
    assert psql.call_count == 3


def test_transform_channel_missing_columns_only_still_returns_true():
    p = _provisioner()
    drift = _drift(missing=["received_at"])
    with patch.object(p, "_psql") as psql:
        assert p._transform_channel(drift) is True
    psql.assert_not_called()


def test_is_non_destructive_always_false():
    p = _provisioner()
    assert p._is_non_destructive() is False
