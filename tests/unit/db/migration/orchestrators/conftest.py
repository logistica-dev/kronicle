# tests/unit/db/migration/orchestrators/conftest.py
"""Shared fixtures for the provisioning unit tests."""

import pytest

from kronicle.deps import settings_env as se


def make_db_settings(
    *,
    host: str = "localhost",
    port: int = 5432,
    db_name: str = "kronicle_unit_test",
    rbac_user: str = "kronicle_rbac",
    rbac_pwd: str = "rbac_pwd",
    chan_user: str = "kronicle_chan",
    chan_pwd: str = "chan_pwd",
    dbsu_user: str | None = "postgres",
    dbsu_pwd: str = "postgres",
) -> se.DBSettings:
    """Build a real DBSettings backed by a synthetic (never-connected) config."""
    chan_creds = se.ChanDbCreds(username=chan_user, password=chan_pwd)
    rbac_creds = se.RbacDbCreds(username=rbac_user, password=rbac_pwd)
    dbsu_creds = se.DbSuCreds(username=dbsu_user, password=dbsu_pwd) if dbsu_user else None
    db = se.DbAccess(host=host, port=port, name=db_name, usr=chan_user, pwd=chan_pwd)
    conf = se.KronicleEnvConf(
        chan_creds=chan_creds,
        rbac_creds=rbac_creds,
        dbsu_creds=dbsu_creds,
        db=db,
        server=se.ConnectionSettings(host=host, port=port),
        env=se.AppEnv(_env="dev"),
        conf_file=None,
    )
    return se.DBSettings(conf)


@pytest.fixture
def db_settings() -> se.DBSettings:
    """A real DBSettings object with inert, non-connectable credentials."""
    return make_db_settings()
