# tests/unit/deps/test_settings.py
from unittest.mock import MagicMock

import pytest

from kronicle.deps import settings as ks
from kronicle.deps.settings_env import (
    AppEnv,
    ChanneDbCreds,
    ConnectionSettings,
    DbAccess,
    KronicleEnvConf,
    RbacDbCreds,
)


@pytest.fixture
def real_env_conf():
    chan_creds = ChanneDbCreds(username="chan_user", password="chan_pass")
    rbac_creds = RbacDbCreds(username="rbac_user", password="rbac_pass")
    db_access = DbAccess(host="127.0.0.1", port=5432, name="kronicle", usr=chan_creds.username, pwd=chan_creds.password)
    server = ConnectionSettings(host="127.0.0.1", port=8080)
    env = AppEnv(_env=AppEnv._ENV_DEV)
    return KronicleEnvConf(
        chan_creds=chan_creds, rbac_creds=rbac_creds, db=db_access, server=server, env=env, conf_file=None
    )


@pytest.fixture(autouse=True)
def patch_from_env(real_env_conf, monkeypatch):
    """Patch KronicleEnvConf.from_env to return the real fixture."""
    monkeypatch.setattr(ks.KronicleEnvConf, "from_env", lambda: real_env_conf)


def test_kronicle_settings_init():
    """Ensure KronicleSettings initializes without errors."""
    settings = ks.KronicleSettings()
    assert settings.server.host == "127.0.0.1"
    assert settings.db._chan_usr == "chan_user"
    assert settings.db._rbac_usr == "rbac_user"
    assert settings.is_dev_env
    assert not settings.is_prod_env
    assert not settings.is_stage_env


def test_kronicle_settings_dict_json():
    """Check .as_dict() and .json() work."""
    settings = ks.KronicleSettings()
    d = settings.as_dict()
    assert isinstance(d, dict)
    j = settings.json()
    assert isinstance(j, str)
    assert "chan_user" in j


@pytest.fixture
def mock_ini_parser(monkeypatch):
    """Mock load_ini_file and SettingsIni classes."""
    fake_parser = MagicMock()
    monkeypatch.setattr(ks, "load_ini_file", MagicMock(return_value=fake_parser))
    monkeypatch.setattr(ks, "AppSettings", MagicMock(from_parser=MagicMock(return_value=MagicMock())))
    monkeypatch.setattr(ks, "JWTSettings", MagicMock(from_parser=MagicMock(return_value=MagicMock())))
    monkeypatch.setattr(ks, "AuthSettings", MagicMock(from_parser=MagicMock(return_value=MagicMock())))
    return fake_parser
