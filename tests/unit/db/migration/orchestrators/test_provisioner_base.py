# tests/unit/db/migration/orchestrators/test_provisioner_base.py
from pathlib import Path
from unittest.mock import patch

import pytest

from kronicle.db.migration.orchestrators.provisioner_base import (
    ApplyResult,
    BaseProvisioner,
    _now_stamp,
    backup_path,
)
from kronicle.deps.settings_env import MigrationSettings


class FakeProvisioner(BaseProvisioner):
    def analyze(self, **kwargs) -> None:
        pass

    def ask_validation(self, **kwargs) -> bool:
        return True

    def backup(self) -> Path | str | None:
        return None

    def restore_backup(self, backup_file: Path | str | None) -> None:
        pass

    def execute_plan(self) -> None:
        pass

    def run_post_analysis(self) -> bool:
        return True


def test_now_stamp_matches_format():
    stamp = _now_stamp()
    assert len(stamp) == 15
    stamp.replace("_", "0").isdigit()


class TestBackupPath:
    def test_creates_dir_and_returns_dump_path(self, tmp_path):
        prefix = str(tmp_path / "workflow_backup")
        with patch("kronicle.db.migration.orchestrators.provisioner_base.access", return_value=True):
            result = backup_path(prefix, "infra", ts="20260101_000000")
        assert result.parent == tmp_path
        assert result == tmp_path / "workflow_backup_infra_20260101_000000.dump"

    def test_uses_current_timestamp_when_not_provided(self, tmp_path):
        prefix = str(tmp_path / "wf")
        with patch("kronicle.db.migration.orchestrators.provisioner_base.access", return_value=True):
            result = backup_path(prefix, "vars")
        assert result.parent == tmp_path
        assert result.name.startswith("wf_vars_")
        assert result.name.endswith(".dump")

    def test_raises_when_dir_not_writable(self, tmp_path):
        with patch("kronicle.db.migration.orchestrators.provisioner_base.access", return_value=False):
            with pytest.raises(RuntimeError, match="not writable"):
                backup_path(str(tmp_path / "wf"), "skip")

    def test_logs_when_dir_creation_fails(self, tmp_path):
        prefix = str(tmp_path / "wf")
        with patch.object(Path, "mkdir", side_effect=OSError("nope")):
            with patch("kronicle.db.migration.orchestrators.provisioner_base.access", return_value=True):
                result = backup_path(prefix, "skip", ts="20260101_000000")
        assert isinstance(result, Path)


class TestApplyResult:
    def test_valid_statuses(self):
        for status in ("ok", "leftovers", "error", "aborted"):
            r = ApplyResult(status=status)
            assert r.status == status

    def test_invalid_status_raises(self):
        with pytest.raises(ValueError, match="invalid status"):
            ApplyResult(status="nope")

    def test_converged_only_for_ok(self):
        assert ApplyResult(status="ok").converged
        assert not ApplyResult(status="leftovers").converged
        assert not ApplyResult(status="error").converged
        assert not ApplyResult(status="aborted").converged

    def test_aborted_flag(self):
        assert ApplyResult(status="aborted").aborted
        assert not ApplyResult(status="ok").aborted

    def test_failed_only_for_error(self):
        assert ApplyResult(status="error").failed
        assert not ApplyResult(status="ok").failed

    def test_defaults(self):
        r = ApplyResult(status="ok")
        assert r.applied_ops == 0
        assert r.safety_level is None
        assert r.revision is None
        assert r.message is None


class TestGetBackupPath:
    def test_delegates_to_backup_path_with_prefix(self):
        p = FakeProvisioner()
        p.migration_settings = MigrationSettings(backup_prefix="/tmp/kronicle_backups")
        with patch(
            "kronicle.db.migration.orchestrators.provisioner_base.backup_path", return_value=Path("x.dump")
        ) as mock_bp:
            assert p.get_backup_path("infra") == Path("x.dump")
        mock_bp.assert_called_once_with("/tmp/kronicle_backups", "infra")
