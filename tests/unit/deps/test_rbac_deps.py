# tests/unit/deps/test_rbac_deps.py
from unittest.mock import MagicMock

from kronicle.deps.rbac_deps import auth_service, core_service, jwt_service, rbac_service


def _request(**state_attrs):
    request = MagicMock()
    for name, value in state_attrs.items():
        setattr(request.app.state, name, value)
    return request


def test_jwt_service_returns_jwt_from_app_state():
    request = _request(jwt_service="the-jwt")
    assert jwt_service(request) == "the-jwt"


def test_auth_service_returns_auth_from_app_state():
    request = _request(auth_service="the-auth")
    assert auth_service(request) == "the-auth"


def test_rbac_service_returns_rbac_from_app_state():
    request = _request(rbac_service="the-rbac")
    assert rbac_service(request) == "the-rbac"


def test_core_service_returns_core_from_app_state():
    request = _request(core_service="the-core")
    assert core_service(request) == "the-core"
