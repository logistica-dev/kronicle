# tests/test_settings_env.py
import base64

import pytest

from kronicle.deps import settings_env as se
from kronicle.utils.str_utils import encode_b64url


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
    creds = se.ChanneDbCreds.from_env()
    assert creds.username == "chan"
    assert creds.password == "pass"

    # Invalid b64
    monkeypatch.setenv(se.CHAN_CREDS, "invalid")
    with pytest.raises(RuntimeError):
        se.ChanneDbCreds.from_env()

    monkeypatch.delenv(se.CHAN_CREDS, raising=False)
    with pytest.raises(RuntimeError):
        se.ChanneDbCreds.from_env()


def test_connection_settings(monkeypatch):
    monkeypatch.delenv(se.APP_HOST, raising=False)
    monkeypatch.delenv(se.APP_PORT, raising=False)
    cs = se.ConnectionSettings.from_env()
    assert cs.host == "0.0.0.0"
    assert cs.port == 8080

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


def test_db_access_dsn(monkeypatch):
    monkeypatch.setenv(se.DB_HOST, "localhost")
    monkeypatch.setenv(se.DB_PORT, "5432")
    monkeypatch.setenv(se.DB_NAME, "mydb")
    default_creds = se.UserCreds(username="user", password="p!_ERZTHOJHBf")
    db = se.DbAccess.from_env(default_creds)
    dsn = db.dsn()
    assert dsn.startswith("postgresql://user:p!_ERZTHOJHBf@localhost:5432/mydb")
    dsn2 = db.dsn(se.UserCreds("x", "y"), db_name="other")
    assert dsn2.startswith("postgresql://x:y@localhost:5432/other")


def test_db_settings(monkeypatch):
    monkeypatch.setenv(se.DB_HOST, "localhost")
    monkeypatch.setenv(se.DB_PORT, "5432")
    monkeypatch.setenv(se.DB_NAME, "mydb")
    val = base64.urlsafe_b64encode(b"chan:pass").decode()
    monkeypatch.setenv(se.CHAN_CREDS, val)
    monkeypatch.setenv(se.RBAC_CREDS, val)
    chan = se.ChanneDbCreds.from_env()
    rbac = se.RbacDbCreds.from_env()
    db = se.DbAccess.from_env(chan)
    conf = se.KronicleEnvConf(
        chan_creds=chan,
        rbac_creds=rbac,
        db=db,
        server=se.ConnectionSettings.from_env(),
        env=se.AppEnv.from_env(),
        conf_file=None,
    )
    settings = se.DBSettings(conf)
    assert "postgresql://" in settings.channel_connection_url
    assert settings.masked_connection_url
    assert settings.masked_connection_url.count("******") == 1
