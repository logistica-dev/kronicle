# tests/unit/deps/test_settings_ini.py
from configparser import ConfigParser
from uuid import UUID

import pytest

import kronicle.deps.settings_ini as si
from kronicle.deps.settings_ini import (
    AppSettings,
    AuthSettings,
    IniSection,
    JWTSettings,
    RbacSettings,
    package_version,
)


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def make_parser(sections: dict[str, dict[str, str]]) -> ConfigParser:
    parser = ConfigParser()
    for section, values in sections.items():
        parser.add_section(section)
        for key, val in values.items():
            parser.set(section, key, val)
    return parser


# ---------------------------------------------------------
# IniSection tests
# ---------------------------------------------------------
def test_inisection_from_parser_and_as_dict():
    class DummySection(IniSection):
        section = "dummy"
        a: str = "default"
        b: int = 123

    parser = make_parser({"dummy": {"a": "hello"}})
    obj = DummySection.from_parser(parser)
    # Only a overridden, b default used
    assert obj.a == "hello"
    assert obj.b == 123
    # as_dict returns correct keys
    d = obj.as_dict()
    assert d["a"] == "hello"
    assert d["b"] == 123
    # json string
    assert isinstance(obj.json(), str)


def test_inisection_missing_section_raises():
    class DummySection(IniSection):
        section = ""
        a: str = "x"

    parser = make_parser({})
    with pytest.raises(ValueError):
        DummySection.from_parser(parser)


# ---------------------------------------------------------
# AppSettings tests
# ---------------------------------------------------------
def test_appsettings_defaults_and_prefix():
    app = AppSettings()
    assert app.version == package_version()
    assert app.version  # resolved from pyproject.toml, never empty
    assert app.name == "Kronicle"
    assert isinstance(app.id, UUID)
    tinyid = app.tinyid
    assert len(tinyid) == 8 + 4  # 8+4 chars
    assert app.prefix.startswith("krcl_")


def test_appsettings_from_parser_override():
    parser = make_parser({"app": {"version": "1.2.3", "name": "TestApp"}})
    app = AppSettings.from_parser(parser)
    assert app.version == "1.2.3"
    assert app.name == "TestApp"
    assert (
        app.__str__()
        == '{"version": "1.2.3", "name": "TestApp", "id": "ffffffff-62dd-490a-8f7e-b168c68da4a7", "description": "FastAPI-powered TimescaleDB microservice for storing time-series measurements", "openapi_url": "/openapi", "should_log_request": true}'
    )


# ---------------------------------------------------------
# AuthSettings tests
# ---------------------------------------------------------
def test_authsettings_defaults_and_validator():
    auth = AuthSettings()
    # defaults
    assert auth.pwd_min_length == 12
    assert auth.pwd_require_uppercase is True
    # valid special chars
    valid_chars = "!@_"
    assert auth.validate_special_chars(valid_chars) == valid_chars
    # forbidden chars
    with pytest.raises(ValueError):
        auth.validate_special_chars("bad$char")


def test_authsettings_from_parser_override():
    parser = make_parser({"auth": {"pwd_min_length": "16"}})
    auth = AuthSettings.from_parser(parser)
    assert auth.pwd_min_length == 16


# ---------------------------------------------------------
# JWTSettings tests
# ---------------------------------------------------------
def test_jwtsettings_defaults_and_secret():
    jwt = JWTSettings()
    secret_value = jwt.get_secret()
    assert isinstance(secret_value, str)
    assert len(secret_value) >= 32  # two UUIDs


def test_jwtsettings_algorithm_validator():
    jwt = JWTSettings()
    assert jwt.validate_algorithm("HS256") == "HS256"
    with pytest.raises(ValueError):
        jwt.validate_algorithm("unsupported")


def test_jwtsettings_from_parser_override():
    parser = make_parser({"jwt": {"expiration_minutes": "60", "algorithm": "HS512"}})
    jwt = JWTSettings.from_parser(parser)
    assert jwt.expiration_minutes == 60
    assert jwt.algorithm == "HS512"


# ---------------------------------------------------------
# RbacSettings tests
# ---------------------------------------------------------
def test_rbacsettings_reserved_names_from_csv():
    rbac = RbacSettings(reserved_names="alpha, beta ,, gamma")  # type: ignore
    assert rbac.reserved_names == ["alpha", "beta", "gamma"]


def test_rbacsettings_reserved_names_list_passthrough():
    rbac = RbacSettings(reserved_names=["delta", "epsilon"])
    assert rbac.reserved_names == ["delta", "epsilon"]


# ---------------------------------------------------------
# package_version fallbacks
# ---------------------------------------------------------
def test_package_version_malformed_pyproject_falls_back_to_metadata(tmp_path, monkeypatch):
    (tmp_path / "pyproject.toml").write_text("[[[ not valid toml")
    monkeypatch.setattr(si, "resolve_project_root", lambda *args, **kwargs: tmp_path)
    monkeypatch.setattr(si.importlib.metadata, "version", lambda name: "9.9.9")
    assert si.package_version() == "9.9.9"


def test_package_version_missing_version_key_falls_back_to_metadata(tmp_path, monkeypatch):
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "kronicle"\n')
    monkeypatch.setattr(si, "resolve_project_root", lambda *args, **kwargs: tmp_path)
    monkeypatch.setattr(si.importlib.metadata, "version", lambda name: "9.9.9")
    assert si.package_version() == "9.9.9"


def test_package_version_unknown_fallback(tmp_path, monkeypatch):
    # No pyproject.toml and no installed distribution metadata -> UNKNOWN_VERSION
    monkeypatch.setattr(si, "resolve_project_root", lambda *args, **kwargs: tmp_path)

    def _raise(name):
        raise si.PackageNotFoundError

    monkeypatch.setattr(si.importlib.metadata, "version", _raise)
    assert si.package_version() == si.UNKNOWN_VERSION
