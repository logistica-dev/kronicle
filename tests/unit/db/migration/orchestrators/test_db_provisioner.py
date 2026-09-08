# tests/unit/db/migration/orchestrators/test_db_provisioner.py
"""Unit tests for kronicle.db.migration.orchestrators.db_provisioner.DbProvisioner."""

from pathlib import Path
from subprocess import CalledProcessError
from unittest.mock import MagicMock, patch

import pytest

from kronicle.db.migration.orchestrators import db_provisioner as dp
from kronicle.db.migration.orchestrators.db_provisioner import DbProvisioner

from .conftest import make_db_settings

# ==================================================================================================
# Helpers
# ==================================================================================================


def _completed(stdout: str = ""):
    return MagicMock(stdout=stdout, stderr="")


def _engine(row):
    """A mocked SQLAlchemy engine whose connection returns ``row`` from ``execute().first()``."""
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.first.return_value = row
    engine.connect.return_value.__enter__.return_value = conn
    return engine


def _provisioner(**settings_kwargs):
    return DbProvisioner(db_settings=make_db_settings(**settings_kwargs))


# ==================================================================================================
# _can_connect
# ==================================================================================================


def test_can_connect_success():
    p = _provisioner()
    with patch.object(dp, "create_engine", return_value=_engine((1,))) as eng:
        assert p._can_connect("postgresql://u:p@localhost:5432/kronicle_unit_test", "app") is True
    eng.assert_called_once()


def test_can_connect_failure_logs_and_returns_false():
    p = _provisioner()
    with patch.object(dp, "create_engine", side_effect=RuntimeError("boom")):
        assert p._can_connect("postgresql://u:p@localhost:5432/kronicle_unit_test", "app") is False


# ==================================================================================================
# Atomic checks
# ==================================================================================================


def test_check_db_exists_true():
    p = _provisioner()
    with patch.object(dp, "subprocess") as sp:
        sp.run.return_value = _completed("1\n")
        assert p.check_db_exists("postgresql://postgres:postgres@localhost:5432/postgres") is True
    cmd = sp.run.call_args.args[0]
    assert "psql" in cmd and f"SELECT 1 FROM pg_database WHERE datname = '{p.db_name}'" in cmd


def test_check_db_exists_false():
    p = _provisioner()
    with patch.object(dp, "subprocess") as sp:
        sp.run.return_value = _completed("0\n")
        assert p.check_db_exists("postgresql://postgres:postgres@localhost:5432/postgres") is False


def test_check_psql_user_exists_with_url():
    p = _provisioner()
    with patch.object(dp, "subprocess") as sp:
        sp.run.return_value = _completed("1\n")
        assert p.check_psql_user_exists("kronicle_rbac", "postgresql://postgres:p@localhost/postgres") is True
    cmd = sp.run.call_args.args[0]
    assert "SELECT 1 FROM pg_roles WHERE rolname = 'kronicle_rbac'" in cmd


def test_check_psql_user_exists_without_url_falls_back_to_connect():
    p = _provisioner(dbsu_user=None)
    with patch.object(p, "_can_connect", return_value=True) as can:
        assert p.check_psql_user_exists("kronicle_rbac", None) is True
    can.assert_called_once_with(
        "postgresql://kronicle_rbac:rbac_pwd@localhost:5432/kronicle_unit_test", "owner 'kronicle_rbac'"
    )


def test_check_psql_user_connectable():
    p = _provisioner()
    with patch.object(dp, "create_engine", return_value=_engine((1,))):
        assert p.check_psql_user_connectable("kronicle_rbac") is True


def test_check_db_create_privilege_true():
    p = _provisioner()
    with patch.object(dp, "create_engine", return_value=_engine((True,))) as eng:
        assert (
            p.check_db_create_privilege("kronicle_rbac", "postgresql://p:secret@localhost:5432/kronicle_unit_test")
            is True
        )
    stmt = eng.return_value.connect.return_value.__enter__.return_value.execute.call_args.args[0]
    assert "has_database_privilege" in str(stmt)


def test_check_db_create_privilege_exception_returns_false():
    p = _provisioner()
    with patch.object(dp, "create_engine", side_effect=RuntimeError("boom")):
        assert (
            p.check_db_create_privilege("kronicle_rbac", "postgresql://p:secret@localhost:5432/kronicle_unit_test")
            is False
        )


def test_check_schema_ownership_matches():
    p = _provisioner()
    with patch.object(dp, "create_engine", return_value=_engine(("kronicle_rbac",))):
        assert (
            p.check_schema_ownership("core", "kronicle_rbac", "postgresql://p:secret@localhost:5432/kronicle_unit_test")
            is True
        )


def test_check_schema_ownership_wrong_owner():
    p = _provisioner()
    with patch.object(dp, "create_engine", return_value=_engine(("other",))):
        assert (
            p.check_schema_ownership("core", "kronicle_rbac", "postgresql://p:secret@localhost:5432/kronicle_unit_test")
            is False
        )


def test_check_schema_ownership_missing():
    p = _provisioner()
    with patch.object(dp, "create_engine", return_value=_engine(None)):
        assert (
            p.check_schema_ownership("core", "kronicle_rbac", "postgresql://p:secret@localhost:5432/kronicle_unit_test")
            is False
        )


def test_check_timescaledb_extension_installed():
    p = _provisioner()
    with patch.object(dp, "create_engine", return_value=_engine((1,))):
        assert p.check_timescaledb_extension("postgresql://p:secret@localhost:5432/kronicle_unit_test") is True


def test_check_timescaledb_extension_missing():
    p = _provisioner()
    with patch.object(dp, "create_engine", return_value=_engine(None)):
        assert p.check_timescaledb_extension("postgresql://p:secret@localhost:5432/kronicle_unit_test") is False


def test_schema_info_exists():
    p = _provisioner()
    with patch.object(dp, "create_engine", return_value=_engine(("kronicle_rbac",))):
        assert p._schema_info("core", "postgresql://p:secret@localhost:5432/kronicle_unit_test") == (
            True,
            "kronicle_rbac",
        )


def test_schema_info_exception_returns_missing():
    p = _provisioner()
    with patch.object(dp, "create_engine", side_effect=RuntimeError("boom")):
        assert p._schema_info("core", "postgresql://p:secret@localhost:5432/kronicle_unit_test") == (False, None)


# ==================================================================================================
# check_readiness aggregation
# ==================================================================================================


def test_check_readiness_all_satisfied():
    p = _provisioner()
    with (
        patch.object(DbProvisioner, "check_db_exists", return_value=True),
        patch.object(DbProvisioner, "check_psql_user_exists", return_value=True),
        patch.object(DbProvisioner, "check_psql_user_connectable", return_value=True),
        patch.object(DbProvisioner, "check_schema_ownership", return_value=True),
        patch.object(DbProvisioner, "check_db_create_privilege", return_value=True),
        patch.object(DbProvisioner, "check_timescaledb_extension", return_value=True),
    ):
        assert p.check_readiness() == {}


def test_check_readiness_reports_missing_db_and_short_circuits():
    p = _provisioner()
    with (
        patch.object(DbProvisioner, "check_db_exists", return_value=False),
        patch.object(DbProvisioner, "check_psql_user_exists", return_value=True),
        patch.object(DbProvisioner, "check_schema_ownership") as schemas,
        patch.object(DbProvisioner, "check_timescaledb_extension") as ext,
    ):
        missing = p.check_readiness()
    assert "db" in missing
    assert "kronicle_unit_test" in missing["db"][0]
    schemas.assert_not_called()
    ext.assert_not_called()


def test_check_readiness_reports_missing_owner_role():
    p = _provisioner()
    with (
        patch.object(DbProvisioner, "check_db_exists", return_value=True),
        patch.object(DbProvisioner, "check_psql_user_exists", return_value=False),
        patch.object(DbProvisioner, "check_schema_ownership", return_value=True),
        patch.object(DbProvisioner, "check_db_create_privilege", return_value=True),
        patch.object(DbProvisioner, "check_timescaledb_extension", return_value=True),
    ):
        missing = p.check_readiness()
    assert "users" in missing
    assert any("missing owner role 'kronicle_rbac'" in item for item in missing["users"])


def test_check_readiness_reports_unconnectable_owner_role():
    p = _provisioner()
    with (
        patch.object(DbProvisioner, "check_db_exists", return_value=True),
        patch.object(DbProvisioner, "check_psql_user_exists", return_value=True),
        patch.object(DbProvisioner, "check_psql_user_connectable", return_value=False),
        patch.object(DbProvisioner, "check_schema_ownership", return_value=True),
        patch.object(DbProvisioner, "check_db_create_privilege", return_value=True),
        patch.object(DbProvisioner, "check_timescaledb_extension", return_value=True),
    ):
        missing = p.check_readiness()
    assert any("cannot connect" in item for item in missing["users"])


def test_check_readiness_reports_bad_schema_ownership():
    p = _provisioner()
    with (
        patch.object(DbProvisioner, "check_db_exists", return_value=True),
        patch.object(DbProvisioner, "check_psql_user_exists", return_value=True),
        patch.object(DbProvisioner, "check_psql_user_connectable", return_value=True),
        patch.object(DbProvisioner, "check_schema_ownership", return_value=False),
        patch.object(DbProvisioner, "check_db_create_privilege", return_value=True),
        patch.object(DbProvisioner, "check_timescaledb_extension", return_value=True),
    ):
        missing = p.check_readiness()
    assert "schemas" in missing
    assert all("not owned by" in item for item in missing["schemas"])


def test_check_readiness_reports_missing_create_privilege():
    p = _provisioner()
    with (
        patch.object(DbProvisioner, "check_db_exists", return_value=True),
        patch.object(DbProvisioner, "check_psql_user_exists", return_value=True),
        patch.object(DbProvisioner, "check_psql_user_connectable", return_value=True),
        patch.object(DbProvisioner, "check_schema_ownership", return_value=True),
        patch.object(DbProvisioner, "check_db_create_privilege", return_value=False),
        patch.object(DbProvisioner, "check_timescaledb_extension", return_value=True),
    ):
        missing = p.check_readiness()
    assert "privileges" in missing
    assert len(missing["privileges"]) == 2


def test_check_readiness_reports_missing_extension():
    p = _provisioner()
    with (
        patch.object(DbProvisioner, "check_db_exists", return_value=True),
        patch.object(DbProvisioner, "check_psql_user_exists", return_value=True),
        patch.object(DbProvisioner, "check_psql_user_connectable", return_value=True),
        patch.object(DbProvisioner, "check_schema_ownership", return_value=True),
        patch.object(DbProvisioner, "check_db_create_privilege", return_value=True),
        patch.object(DbProvisioner, "check_timescaledb_extension", return_value=False),
    ):
        missing = p.check_readiness()
    assert "extension" in missing


def test_check_readiness_without_dbsu_probes_rbac():
    p = _provisioner(dbsu_user=None)
    with (
        patch.object(DbProvisioner, "check_schema_ownership", return_value=True),
        patch.object(DbProvisioner, "check_db_create_privilege", return_value=True),
        patch.object(DbProvisioner, "check_timescaledb_extension", return_value=True),
    ):
        # db probe + 2x user existence (no-url branch) + 2x user connectability
        with patch.object(p, "_can_connect", side_effect=[True] * 5) as can:
            assert p.check_readiness() == {}
    assert len(can.call_args_list) == 5


def test_check_readiness_without_dbsu_unreachable_db():
    p = _provisioner(dbsu_user=None)
    with (
        patch.object(DbProvisioner, "check_schema_ownership") as schemas,
        patch.object(DbProvisioner, "check_timescaledb_extension") as ext,
    ):
        with patch.object(p, "_can_connect", return_value=False) as can:
            missing = p.check_readiness()
    assert "db" in missing
    # db probe + user existence probes still run; schema/privilege/extension are skipped
    assert len(can.call_args_list) == 3
    schemas.assert_not_called()
    ext.assert_not_called()


# ==================================================================================================
# backup / restore
# ==================================================================================================


def test_backup_requires_dbsu():
    p = _provisioner(dbsu_user=None)
    with pytest.raises(RuntimeError, match="dbsu_url"):
        p.backup()


def test_backup_skips_when_db_missing():
    p = _provisioner()
    with (
        patch.object(DbProvisioner, "check_db_exists", return_value=False),
        patch.object(dp, "subprocess") as sp,
    ):
        assert p.backup() is None
    sp.run.assert_not_called()


def test_backup_dumps_managed_schemas():
    p = _provisioner()
    backup_file = Path("/tmp/kronicle_backup.dump")
    with (
        patch.object(DbProvisioner, "check_db_exists", return_value=True),
        patch.object(dp, "get_env_var", return_value="/tmp/kronicle_backup"),
        patch.object(dp, "backup_path", return_value=backup_file),
        patch.object(dp, "subprocess") as sp,
    ):
        result = p.backup()
    assert result == backup_file
    cmd = sp.run.call_args.args[0]
    assert cmd[0] == "pg_dump"
    assert "-Fc" in cmd
    assert str(backup_file) in cmd
    assert any(schema in cmd for schema in p.schema_owners)
    assert backup_file.parent.exists()


def test_backup_propagates_failure():
    p = _provisioner()
    with (
        patch.object(DbProvisioner, "check_db_exists", return_value=True),
        patch.object(dp, "get_env_var", return_value="/tmp/kronicle_backup"),
        patch.object(dp, "backup_path", return_value=Path("/tmp/kronicle_backup.dump")),
        patch(
            "kronicle.db.migration.orchestrators.db_provisioner.subprocess.run",
            side_effect=CalledProcessError(1, "pg_dump", stderr="disk full"),
        ),
    ):
        with pytest.raises(RuntimeError, match="Backup failed"):
            p.backup()


def test_restore_backup_is_noop():
    p = _provisioner()
    p.restore_backup(None)
    p.restore_backup(Path("/tmp/kronicle_backup.dump"))


# ==================================================================================================
# analyze / ask_validation
# ==================================================================================================


def test_analyze_sets_state_and_has_work():
    p = _provisioner()
    with patch.object(DbProvisioner, "check_readiness", return_value={"db": ["missing"]}):
        p.analyze()
    assert p._has_work is True
    assert p._missing == {"db": ["missing"]}
    assert p._auto_approve is False


def test_analyze_no_work():
    p = _provisioner()
    with patch.object(DbProvisioner, "check_readiness", return_value={}):
        p.analyze()
    assert p._has_work is False


def test_ask_validation_auto_approve_skips_prompt():
    p = _provisioner()
    p._missing = {"db": ["missing"]}
    p._auto_approve = True
    with patch.object(p, "_confirm") as confirm:
        assert p.ask_validation() is True
    confirm.assert_not_called()


def test_ask_validation_confirmed():
    p = _provisioner()
    p._missing = {"db": ["missing"]}
    p._auto_approve = False
    with patch.object(p, "_confirm", return_value=True):
        assert p.ask_validation() is True


def test_ask_validation_declined():
    p = _provisioner()
    p._missing = {"db": ["missing"]}
    p._auto_approve = False
    with patch.object(p, "_confirm", return_value=False):
        assert p.ask_validation() is False


# ==================================================================================================
# execute_plan + ensure helpers
# ==================================================================================================


def test_execute_plan_requires_dbsu():
    p = _provisioner(dbsu_user=None)
    with pytest.raises(RuntimeError, match="dbsu_url"):
        p.execute_plan()


def test_execute_plan_runs_ensures_and_counts_ops():
    p = _provisioner()
    p._missing = {"db": ["a"], "users": ["b", "c"]}
    with (
        patch.object(DbProvisioner, "check_db_exists", return_value=True),
        patch.object(DbProvisioner, "_ensure_users_exist") as users,
        patch.object(DbProvisioner, "_ensure_database_exists") as db,
        patch.object(DbProvisioner, "_ensure_db_create_privilege") as priv,
        patch.object(DbProvisioner, "_ensure_schemas") as schemas,
        patch.object(DbProvisioner, "_ensure_extension") as ext,
    ):
        p.execute_plan()
    users.assert_called_once()
    db.assert_not_called()
    priv.assert_called_once()
    schemas.assert_called_once()
    ext.assert_called_once()
    assert p._applied_ops == 3


def test_execute_plan_creates_database_when_missing():
    p = _provisioner()
    p._missing = {"db": ["missing"]}
    with (
        patch.object(DbProvisioner, "check_db_exists", return_value=False),
        patch.object(DbProvisioner, "_ensure_users_exist"),
        patch.object(DbProvisioner, "_ensure_database_exists") as db,
        patch.object(DbProvisioner, "_ensure_db_create_privilege"),
        patch.object(DbProvisioner, "_ensure_schemas"),
        patch.object(DbProvisioner, "_ensure_extension"),
    ):
        p.execute_plan()
    db.assert_called_once()


def test_ensure_users_exist_creates_missing_role():
    p = _provisioner()
    with (
        patch.object(DbProvisioner, "check_psql_user_exists", side_effect=[False, True]),
        patch.object(dp, "subprocess") as sp,
    ):
        p._ensure_users_exist("postgresql://postgres:postgres@localhost:5432/kronicle_unit_test")
    assert sp.run.call_count == 1
    cmd = sp.run.call_args.args[0]
    assert "CREATE ROLE \"kronicle_rbac\" LOGIN PASSWORD 'rbac_pwd'" in cmd


def test_ensure_database_exists_creates():
    p = _provisioner()
    with (
        patch.object(DbProvisioner, "check_db_exists", return_value=False),
        patch.object(dp, "subprocess") as sp,
    ):
        p._ensure_database_exists("postgresql://postgres:postgres@localhost:5432/postgres")
    cmd = sp.run.call_args.args[0]
    assert f'CREATE DATABASE "{p.db_name}"' in cmd


def test_ensure_database_exists_skips_when_present():
    p = _provisioner()
    with (
        patch.object(DbProvisioner, "check_db_exists", return_value=True),
        patch.object(dp, "subprocess") as sp,
    ):
        p._ensure_database_exists("postgresql://postgres:postgres@localhost:5432/postgres")
    sp.run.assert_not_called()


def test_ensure_db_create_privilege_grants_to_each_owner():
    p = _provisioner()
    with patch.object(dp, "subprocess") as sp:
        p._ensure_db_create_privilege("postgresql://postgres:postgres@localhost:5432/kronicle_unit_test")
    assert sp.run.call_count == 2
    cmds = [call.args[0] for call in sp.run.call_args_list]
    assert any(f'GRANT CREATE ON DATABASE "{p.db_name}" TO "kronicle_chan"' in cmd for cmd in cmds)
    assert any(f'GRANT CREATE ON DATABASE "{p.db_name}" TO "kronicle_rbac"' in cmd for cmd in cmds)


def test_ensure_schemas_creates_when_missing():
    p = _provisioner()
    with (
        patch.object(DbProvisioner, "_schema_info", return_value=(False, None)),
        patch.object(dp, "subprocess") as sp,
    ):
        p._ensure_schemas("postgresql://postgres:postgres@localhost:5432/kronicle_unit_test")
    assert sp.run.call_count == 3
    cmds = [call.args[0] for call in sp.run.call_args_list]
    assert any('CREATE SCHEMA "core" AUTHORIZATION "kronicle_rbac"' in cmd for cmd in cmds)
    assert any('CREATE SCHEMA "data" AUTHORIZATION "kronicle_chan"' in cmd for cmd in cmds)


def test_ensure_schemas_reassigns_wrong_owner():
    p = _provisioner()
    with (
        patch.object(
            DbProvisioner, "_schema_info", side_effect=[(True, "someone"), (True, "someone"), (True, "someone")]
        ),
        patch.object(dp, "subprocess") as sp,
    ):
        p._ensure_schemas("postgresql://postgres:postgres@localhost:5432/kronicle_unit_test")
    assert sp.run.call_count == 3
    cmds = [call.args[0] for call in sp.run.call_args_list]
    assert any('ALTER SCHEMA "core" OWNER TO "kronicle_rbac"' in cmd for cmd in cmds)
    assert any('ALTER SCHEMA "data" OWNER TO "kronicle_chan"' in cmd for cmd in cmds)


def test_ensure_schemas_keeps_correct_owner():
    p = _provisioner()
    owners = [p.schema_owners[k] for k in ("core", "rbac", "data")]
    with (
        patch.object(DbProvisioner, "_schema_info", side_effect=[(True, o) for o in owners]),
        patch.object(dp, "subprocess") as sp,
    ):
        p._ensure_schemas("postgresql://postgres:postgres@localhost:5432/kronicle_unit_test")
    sp.run.assert_not_called()


def test_ensure_extension_installs_when_missing():
    p = _provisioner()
    with (
        patch.object(DbProvisioner, "check_timescaledb_extension", return_value=False),
        patch.object(dp, "subprocess") as sp,
    ):
        p._ensure_extension("postgresql://postgres:postgres@localhost:5432/kronicle_unit_test")
    assert "CREATE EXTENSION IF NOT EXISTS timescaledb;" in " ".join(sp.run.call_args.args[0])


def test_ensure_extension_skips_when_installed():
    p = _provisioner()
    with (
        patch.object(DbProvisioner, "check_timescaledb_extension", return_value=True),
        patch.object(dp, "subprocess") as sp,
    ):
        p._ensure_extension("postgresql://postgres:postgres@localhost:5432/kronicle_unit_test")
    sp.run.assert_not_called()


# ==================================================================================================
# run_once (BaseProvisioner driver)
# ==================================================================================================


def test_run_once_ok_without_work():
    p = _provisioner()
    with patch.object(DbProvisioner, "check_readiness", return_value={}):
        result = p.run_once()
    assert result.converged is True
    assert result.applied_ops == 0


def test_run_once_applies_and_converges():
    p = _provisioner()
    with (
        patch.object(DbProvisioner, "check_readiness", side_effect=[{"db": ["application database missing"]}, {}]),
        patch.object(DbProvisioner, "check_db_exists", return_value=True),
        patch.object(DbProvisioner, "_ensure_users_exist"),
        patch.object(DbProvisioner, "_ensure_database_exists"),
        patch.object(DbProvisioner, "_ensure_db_create_privilege"),
        patch.object(DbProvisioner, "_ensure_schemas"),
        patch.object(DbProvisioner, "_ensure_extension"),
        patch.object(dp, "get_env_var", return_value="/tmp/kronicle_backup"),
        patch.object(dp, "backup_path", return_value=Path("/tmp/kronicle_backup.dump")),
        patch.object(dp, "subprocess") as sp,
    ):
        sp.run.return_value = _completed("1\n")
        result = p.run_once(auto_approve=True)
    assert result.converged is True
    assert result.applied_ops == 1
    pg_dump = [call.args[0][0] for call in sp.run.call_args_list]
    assert "pg_dump" in pg_dump


def test_run_once_aborted_on_decline():
    p = _provisioner()
    with (
        patch.object(DbProvisioner, "check_readiness", return_value={"db": ["application database missing"]}),
        patch.object(p, "_confirm", return_value=False),
    ):
        result = p.run_once()
    assert result.aborted is True
    assert result.converged is False


def test_run_once_error_restores_and_reports():
    p = _provisioner()
    with (
        patch.object(DbProvisioner, "check_readiness", return_value={"db": ["application database missing"]}),
        patch.object(DbProvisioner, "_confirm", return_value=True),
        patch.object(DbProvisioner, "check_db_exists") as db_exists,
        patch.object(dp, "get_env_var", return_value="/tmp/kronicle_backup"),
        patch.object(dp, "backup_path", return_value=Path("/tmp/kronicle_backup.dump")),
        patch.object(dp, "subprocess"),
        patch.object(DbProvisioner, "restore_backup") as restore,
    ):
        db_exists.side_effect = RuntimeError("connection dropped")
        result = p.run_once()
    assert result.failed is True
    assert restore.call_count == 1
