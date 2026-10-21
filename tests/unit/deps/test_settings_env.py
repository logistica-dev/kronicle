# tests/unit/deps/test_settings_env.py
import base64
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from kronicle.deps import settings_env as se
from kronicle.utils.str_utils import encode_b64url


def test_resolve_project_root_finds_pyproject_from_nested_dir(tmp_path):
    root = tmp_path / "proj"
    root.mkdir()
    (root / "pyproject.toml").write_text("")
    nested = root / "src" / "sub"
    nested.mkdir(parents=True)
    assert se.resolve_project_root(nested) == root.resolve()


def test_resolve_project_root_falls_back_to_start(tmp_path):
    assert se.resolve_project_root(tmp_path) == tmp_path.resolve()


def test_resolve_backup_prefix_absolute_passthrough():
    assert se.resolve_backup_prefix("/tmp/kronicle_backups") == "/tmp/kronicle_backups"


def test_resolve_backup_prefix_bare_name_joins_host_dir():
    assert se.resolve_backup_prefix("series", "/abs/dir") == "/abs/dir/series"


def test_resolve_backup_prefix_bare_name_defaults_host_dir(tmp_path, monkeypatch):
    root = tmp_path / "proj"
    root.mkdir()
    (root / "pyproject.toml").write_text("")
    (root / "src").mkdir()
    monkeypatch.chdir(root / "src")
    assert se.resolve_backup_prefix("kronicle") == str(root / "backup" / "kronicle")


def test_resolve_backup_prefix_relative_uses_project_root(tmp_path, monkeypatch):
    root = tmp_path / "proj"
    root.mkdir()
    (root / "pyproject.toml").write_text("")
    (root / "backup").mkdir()
    (root / "src").mkdir()
    monkeypatch.chdir(root / "src")
    assert se.resolve_backup_prefix("./backup/kronicle") == str(root / "backup" / "kronicle")


def test_migration_settings_normalizes_relative_default(tmp_path, monkeypatch):
    root = tmp_path / "proj"
    root.mkdir()
    (root / "pyproject.toml").write_text("")
    (root / "backup").mkdir()
    (root / "src").mkdir()
    monkeypatch.chdir(root / "src")
    assert Path(se.MigrationSettings().backup_prefix).parent == root / "backup"
    assert Path(se.MigrationSettings.from_env().backup_prefix).parent == root / "backup"
    assert se.MigrationSettings(backup_prefix="/abs/prefix").backup_prefix == "/abs/prefix"
    assert se.MigrationSettings(backup_prefix="kronicle", backup_dir="./bk").backup_prefix == str(
        root / "bk" / "kronicle"
    )


def test_get_env_var(monkeypatch):
    monkeypatch.delenv("FOO", raising=False)
    assert se.get_env_var("FOO", "default") == "default"
    monkeypatch.setenv("FOO", "BAR")
    assert se.get_env_var("FOO", "default") == "BAR"


def test_ensure_env_var(monkeypatch):
    monkeypatch.setenv("FOO", "BAR")
    assert se.ensure_env_var("FOO") == "BAR"
    monkeypatch.delenv("FOO", raising=False)
    with pytest.raises(RuntimeError):
        se.ensure_env_var("FOO")


def test_user_creds_from_env(monkeypatch):
    monkeypatch.setenv("USR", "alice")
    monkeypatch.setenv("PWD", encode_b64url("secret"))
    creds = se.UserCreds.from_env("USR", "PWD")
    assert creds.username == "alice"
    assert creds.password == "c2VjcmV0"

    monkeypatch.delenv("USR", raising=False)
    monkeypatch.delenv("PWD", raising=False)
    with pytest.raises(RuntimeError):
        se.UserCreds.from_env("USR", "PWD")


def test_env_user_creds(monkeypatch):
    val = base64.urlsafe_b64encode(b"chan:pass").decode()
    monkeypatch.setenv(se.CHAN_CREDS, val)
    creds = se.ChanDbCreds.from_env()
    assert creds.username == "chan"
    assert creds.password == "pass"

    # Invalid b64
    monkeypatch.setenv(se.CHAN_CREDS, "invalid")
    with pytest.raises(RuntimeError):
        se.ChanDbCreds.from_env()

    monkeypatch.delenv(se.CHAN_CREDS, raising=False)
    with pytest.raises(RuntimeError):
        se.ChanDbCreds.from_env()


def test_connection_settings(monkeypatch):
    monkeypatch.delenv(se.APP_HOST, raising=False)
    monkeypatch.delenv(se.APP_PORT, raising=False)
    cs = se.ConnectionSettings.from_env()
    assert cs.host == "0.0.0.0"
    assert cs.port == 8000

    monkeypatch.setenv(se.APP_HOST, "127.0.0.1")
    monkeypatch.setenv(se.APP_PORT, "5000")
    cs = se.ConnectionSettings.from_env()
    assert cs.host == "127.0.0.1"
    assert cs.port == 5000


def test_app_env(monkeypatch):
    monkeypatch.setenv(se.KRONICLE_ENV, "dev")
    env = se.AppEnv.from_env()
    assert env.is_dev_env is True
    assert env.is_prod_env is False

    monkeypatch.setenv(se.KRONICLE_ENV, "production")
    env = se.AppEnv.from_env()
    assert env.is_prod_env is True

    monkeypatch.setenv(se.KRONICLE_ENV, "stage")
    env = se.AppEnv.from_env()
    assert env.is_stage_env is True

    monkeypatch.setenv(se.KRONICLE_ENV, "unknown")
    with pytest.raises(ValueError):
        se.AppEnv.from_env()


def test_db_access_profile_dsn(monkeypatch):
    monkeypatch.setenv(se.DB_HOST, "localhost")
    monkeypatch.setenv(se.DB_PORT, "5432")
    monkeypatch.setenv(se.DB_NAME, "mydb")
    monkeypatch.setenv(se.DB_NAME_ALT, "mydb")
    default_creds = se.UserCreds(username="user", password="p!_ERZTHOJHBf")
    db = se.DbAccess.from_env(default_creds)
    dsn = db.dsn()
    assert dsn.startswith("postgresql://user:p!_ERZTHOJHBf@localhost:5432/mydb")
    dsn2 = db.dsn(se.UserCreds("x", "y"), db_name="other")
    assert dsn2.startswith("postgresql://x:y@localhost:5432/other")


def test_db_settings(monkeypatch):
    monkeypatch.setenv(se.DB_HOST, "localhost")
    monkeypatch.setenv(se.DB_PORT, "5432")
    monkeypatch.setenv(se.DB_NAME_ALT, "mydb")
    val = base64.urlsafe_b64encode(b"chan:pass").decode()
    monkeypatch.setenv(se.CHAN_CREDS, val)
    monkeypatch.setenv(se.RBAC_CREDS, val)
    monkeypatch.setenv(se.DBSU_CREDS, val)
    chan = se.ChanDbCreds.from_env()
    rbac = se.RbacDbCreds.from_env()
    dbsu = se.DbSuCreds.from_env()
    db = se.DbAccess.from_env(chan)
    conf = se.KronicleEnvConf(
        chan_creds=chan,
        rbac_creds=rbac,
        dbsu_creds=dbsu,
        db=db,
        server=se.ConnectionSettings.from_env(),
        env=se.AppEnv.from_env(),
        conf_file=None,
        migration=se.MigrationSettings(),
    )
    settings = se.DBSettings(conf)
    assert "postgresql://" in settings.channel_connection_url
    assert settings.masked_connection_url
    assert settings.masked_connection_url.count("******") == 1


# --------------------------------------------------------------------------------------------------
# UserCreds.creds
# --------------------------------------------------------------------------------------------------
def test_user_creds_creds_property():
    creds = se.UserCreds(username="alice", password="s3cret")
    assert creds.creds == ("alice", "s3cret")


# --------------------------------------------------------------------------------------------------
# AppSuperuser
# --------------------------------------------------------------------------------------------------
def test_app_superuser_from_env(monkeypatch):
    raw = "root:root@example.com:$argon2id$v=19$m=65536,t=3,p=4$c2FsdA$aGFzaA"
    monkeypatch.setenv(se.APP_SU_INFO, encode_b64url(raw))
    su = se.AppSuperuser.from_env()
    assert su.username == "root"
    assert su.email == "root@example.com"
    assert su.password_hash.startswith("$argon2id$")


def test_app_superuser_from_env_missing(monkeypatch):
    monkeypatch.delenv(se.APP_SU_INFO, raising=False)
    with pytest.raises(RuntimeError, match="KRONICLE_SU_INFO"):
        se.AppSuperuser.from_env()


def test_app_superuser_from_env_malformed(monkeypatch):
    # Decodes fine but does not contain "user:email:hash" -> unpacking ValueError -> RuntimeError
    monkeypatch.setenv(se.APP_SU_INFO, encode_b64url("only-a-username"))
    with pytest.raises(RuntimeError, match="KRONICLE_SU_INFO"):
        se.AppSuperuser.from_env()


# --------------------------------------------------------------------------------------------------
# DbAccess.from_env validation
# --------------------------------------------------------------------------------------------------
@pytest.mark.parametrize("bad_name", ["bad name", "1leading", "semi;colon"])
def test_db_access_invalid_db_name_raises(monkeypatch, bad_name):
    monkeypatch.setenv(se.DB_HOST, "localhost")
    monkeypatch.setenv(se.DB_PORT, "5432")
    monkeypatch.setenv(se.DB_NAME, bad_name)
    with pytest.raises(RuntimeError):
        se.DbAccess.from_env(se.UserCreds(username="user", password="pwd"))


# --------------------------------------------------------------------------------------------------
# DbAccess.session
# --------------------------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_db_access_session(monkeypatch):
    mock_conn = AsyncMock()
    mock_connect = AsyncMock(return_value=mock_conn)
    monkeypatch.setattr(se, "connect", mock_connect)
    db = se.DbAccess(host="dbhost", port=5432, name="kronicle", usr="chan", pwd="cpass")

    async with db.session() as conn:
        assert conn is mock_conn
    mock_connect.assert_awaited_once_with(host="dbhost", port=5432, user="chan", password="cpass", database="kronicle")
    mock_conn.close.assert_awaited_once()

    async with db.session(creds=se.UserCreds("rbac", "rpass"), db_name="other"):
        pass
    mock_connect.assert_awaited_with(host="dbhost", port=5432, user="rbac", password="rpass", database="other")
    assert mock_conn.close.await_count == 2


# --------------------------------------------------------------------------------------------------
# KronicleEnvConf.from_env
# --------------------------------------------------------------------------------------------------
def _set_full_env(monkeypatch):
    monkeypatch.setenv(se.DB_HOST, "localhost")
    monkeypatch.setenv(se.DB_PORT, "5432")
    monkeypatch.setenv(se.DB_NAME, "kronicle")
    monkeypatch.setenv(se.CHAN_CREDS, encode_b64url("chan_usr:chan_pwd"))
    monkeypatch.setenv(se.RBAC_CREDS, encode_b64url("rbac_usr:rbac_pwd"))
    monkeypatch.setenv(se.KRONICLE_ENV, "dev")
    monkeypatch.delenv(se.APP_HOST, raising=False)
    monkeypatch.delenv(se.APP_PORT, raising=False)
    monkeypatch.delenv(se.KRONICLE_CONF, raising=False)
    monkeypatch.delenv(se.KRONICLE_DB_AUTO_MIGRATION, raising=False)
    monkeypatch.delenv(se.KRONICLE_BACKUP_PREFIX, raising=False)
    monkeypatch.delenv(se.KRONICLE_BACKUP_DIR, raising=False)


def test_kronicle_env_conf_from_env(monkeypatch):
    _set_full_env(monkeypatch)
    monkeypatch.setenv(se.DBSU_CREDS, encode_b64url("su_usr:su_pwd"))
    conf = se.KronicleEnvConf.from_env()
    assert conf.chan_creds.username == "chan_usr"
    assert conf.rbac_creds.username == "rbac_usr"
    assert conf.dbsu_creds is not None
    assert conf.dbsu_creds.username == "su_usr"
    assert conf.db.host == "localhost"
    assert conf.db.port == 5432
    assert conf.db.name == "kronicle"
    assert conf.server.host == "0.0.0.0"
    assert conf.server.port == 8000
    assert conf.env.is_dev_env
    assert conf.conf_file is None
    assert conf.migration.auto is False


def test_kronicle_env_conf_from_env_without_dbsu(monkeypatch):
    _set_full_env(monkeypatch)
    monkeypatch.delenv(se.DBSU_CREDS, raising=False)
    conf = se.KronicleEnvConf.from_env()
    assert conf.dbsu_creds is None


# --------------------------------------------------------------------------------------------------
# DBSettings masked URLs / dumps
# --------------------------------------------------------------------------------------------------
def test_db_settings_host_port_and_masked_urls(monkeypatch):
    _set_full_env(monkeypatch)
    monkeypatch.setenv(se.DBSU_CREDS, encode_b64url("su_usr:su_pwd"))
    chan = se.ChanDbCreds.from_env()
    rbac = se.RbacDbCreds.from_env()
    dbsu = se.DbSuCreds.from_env()
    db = se.DbAccess.from_env(chan)
    conf = se.KronicleEnvConf(
        chan_creds=chan,
        rbac_creds=rbac,
        dbsu_creds=dbsu,
        db=db,
        server=se.ConnectionSettings.from_env(),
        env=se.AppEnv.from_env(),
        conf_file=None,
        migration=se.MigrationSettings(),
    )
    settings = se.DBSettings(conf)
    assert settings.host == "localhost"
    assert settings.port == 5432
    assert settings.masked_rbac_connection_url.count("******") == 1
    assert settings.masked_dbsu_connection_url is not None
    assert settings.masked_dbsu_connection_url.count("******") == 1
    assert settings.masked_maintenance_url is not None
    assert settings.masked_maintenance_url.endswith("/postgres")
    assert settings.masked_maintenance_url.count("******") == 1
    dumped = settings.model_dump_json()
    assert '"connection_url"' in dumped
    assert str(settings).startswith("DBSettings(")


def test_db_settings_without_dbsu(monkeypatch):
    _set_full_env(monkeypatch)
    monkeypatch.delenv(se.DBSU_CREDS, raising=False)
    chan = se.ChanDbCreds.from_env()
    rbac = se.RbacDbCreds.from_env()
    db = se.DbAccess.from_env(chan)
    conf = se.KronicleEnvConf(
        chan_creds=chan,
        rbac_creds=rbac,
        dbsu_creds=None,
        db=db,
        server=se.ConnectionSettings.from_env(),
        env=se.AppEnv.from_env(),
        conf_file=None,
        migration=se.MigrationSettings(),
    )
    settings = se.DBSettings(conf)
    assert settings.db_name == "kronicle"
    assert settings.rbac_user == "rbac_usr"
    assert settings.chan_user == "chan_usr"
    assert set(settings.owner_passwords) == {"rbac_usr", "chan_usr"}
    assert settings.dbsu_connection_url is None
    assert settings.dbsu_maintenance_url is None
    assert settings.masked_dbsu_connection_url is None
    assert settings.masked_maintenance_url is None
