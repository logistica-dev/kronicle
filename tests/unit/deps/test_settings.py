# tests/unit/deps/test_settings.py
from unittest.mock import MagicMock

import pytest

from kronicle.deps import settings as ks
from kronicle.deps.settings_env import (
    AppEnv,
    ChanDbCreds,
    ConnectionSettings,
    DbAccess,
    KronicleEnvConf,
    MigrationSettings,
    RbacDbCreds,
)


@pytest.fixture
def real_env_conf():
    chan_creds = ChanDbCreds(username="chan_user", password="chan_pass")
    rbac_creds = RbacDbCreds(username="rbac_user", password="rbac_pass")
    db_access_profile = DbAccess(
        host="127.0.0.1", port=5432, name="kronicle", usr=chan_creds.username, pwd=chan_creds.password
    )
    server = ConnectionSettings(host="127.0.0.1", port=8000)
    env = AppEnv(_env=AppEnv._ENV_DEV)
    return KronicleEnvConf(
        chan_creds=chan_creds,
        rbac_creds=rbac_creds,
        db=db_access_profile,
        server=server,
        env=env,
        conf_file=None,
        migration=MigrationSettings(),
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


def test_no_conf_file_path_raises(monkeypatch):
    """No candidate INI file exists -> RuntimeError."""
    monkeypatch.setattr(ks, "is_file", lambda f: False)
    with pytest.raises(RuntimeError, match="No conf file path found"):
        ks.KronicleSettings()


def test_conf_file_incorrect_raises(monkeypatch):
    """A candidate file exists but cannot be parsed -> RuntimeError('Conf file incorrect')."""
    monkeypatch.setattr(ks, "is_file", lambda f: True)
    monkeypatch.setattr(ks, "load_ini_file", MagicMock(side_effect=ValueError("bad ini")))
    with pytest.raises(RuntimeError, match="Conf file incorrect"):
        ks.KronicleSettings()


def test_api_version():
    settings = ks.KronicleSettings()
    assert settings.api_version == "v1"


def test_str_dumps_json():
    settings = ks.KronicleSettings()
    text = str(settings)
    assert text.startswith("{")
    assert "chan_user" in text


@pytest.fixture
def mock_ini_parser(monkeypatch):
    """Mock load_ini_file and SettingsIni classes."""
    fake_parser = MagicMock()
    monkeypatch.setattr(ks, "load_ini_file", MagicMock(return_value=fake_parser))
    monkeypatch.setattr(ks, "AppSettings", MagicMock(from_parser=MagicMock(return_value=MagicMock())))
    monkeypatch.setattr(ks, "JWTSettings", MagicMock(from_parser=MagicMock(return_value=MagicMock())))
    monkeypatch.setattr(ks, "AuthSettings", MagicMock(from_parser=MagicMock(return_value=MagicMock())))
    return fake_parser
