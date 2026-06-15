# tests/unit/auth/test_require_permission.py
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from fastapi import Request

from kronicle.auth.auth_middleware import require_any_permission, require_permission, require_permission_set
from kronicle.errors.error_types import ForbiddenError
from kronicle.schemas.permissions.permission import Permission, PermissionAction, PermissionTarget


class MockState:
    def __init__(self):
        self._perm_cache: dict = {}


@pytest.fixture
def mock_request():
    request = MagicMock(spec=Request)
    request.state = MockState()
    request.app.state.rbac_service = MagicMock()
    return request


def test_superuser_bypass(mock_request):
    user = {"sub": str(uuid4()), "is_superuser": True}

    dep = require_permission("zone:create")
    result = dep(request=mock_request, user=user)

    assert result == user
    mock_request.app.state.rbac_service.user_has_permission.assert_not_called()


def test_permission_granted(mock_request):
    user_id = uuid4()
    user = {"sub": str(user_id), "is_superuser": False}
    mock_request.app.state.rbac_service.user_has_permission.return_value = True

    dep = require_permission("zone:create")
    result = dep(request=mock_request, user=user)

    assert result == user
    expected_perm = Permission(PermissionTarget.ZONE, PermissionAction.CREATE)
    mock_request.app.state.rbac_service.user_has_permission.assert_called_once_with(user_id, expected_perm)


def test_permission_denied(mock_request):
    user = {"sub": str(uuid4()), "is_superuser": False}
    mock_request.app.state.rbac_service.user_has_permission.return_value = False

    dep = require_permission("zone:create")
    with pytest.raises(ForbiddenError, match="Missing required permission"):
        dep(request=mock_request, user=user)


def test_cache_hit_allows(mock_request):
    user = {"sub": str(uuid4()), "is_superuser": False}
    mock_request.app.state.rbac_service.user_has_permission.return_value = True

    dep = require_permission("zone:create")
    dep(request=mock_request, user=user)
    result = dep(request=mock_request, user=user)

    assert result == user
    assert mock_request.app.state.rbac_service.user_has_permission.call_count == 1


def test_cache_hit_blocks(mock_request):
    user = {"sub": str(uuid4()), "is_superuser": False}
    mock_request.app.state.rbac_service.user_has_permission.return_value = False

    dep = require_permission("zone:create")
    with pytest.raises(ForbiddenError):
        dep(request=mock_request, user=user)

    with pytest.raises(ForbiddenError):
        dep(request=mock_request, user=user)

    assert mock_request.app.state.rbac_service.user_has_permission.call_count == 1


# ==============================================================================
# require_permission_set (AND)
# ==============================================================================


def test_permission_set_all_pass(mock_request):
    user = {"sub": str(uuid4()), "is_superuser": False}
    mock_request.app.state.rbac_service.user_has_permission.return_value = True

    dep = require_permission_set("channel:read", "channel:create")
    result = dep(request=mock_request, user=user)

    assert result == user
    assert mock_request.app.state.rbac_service.user_has_permission.call_count == 2


def test_permission_set_one_fails(mock_request):
    user = {"sub": str(uuid4()), "is_superuser": False}
    mock_request.app.state.rbac_service.user_has_permission.side_effect = [True, False]

    dep = require_permission_set("channel:read", "channel:create")
    with pytest.raises(ForbiddenError, match="channel:create"):
        dep(request=mock_request, user=user)


def test_permission_set_superuser_bypass(mock_request):
    user = {"sub": str(uuid4()), "is_superuser": True}

    dep = require_permission_set("channel:read", "channel:create")
    result = dep(request=mock_request, user=user)

    assert result == user
    mock_request.app.state.rbac_service.user_has_permission.assert_not_called()


# ==============================================================================
# require_any_permission (OR)
# ==============================================================================


def test_any_permission_first_match(mock_request):
    user = {"sub": str(uuid4()), "is_superuser": False}
    mock_request.app.state.rbac_service.user_has_permission.return_value = True

    dep = require_any_permission("channel:read", "row:read")
    result = dep(request=mock_request, user=user)

    assert result == user
    mock_request.app.state.rbac_service.user_has_permission.assert_called_once()


def test_any_permission_second_match(mock_request):
    user = {"sub": str(uuid4()), "is_superuser": False}
    mock_request.app.state.rbac_service.user_has_permission.side_effect = [False, True]

    dep = require_any_permission("channel:read", "row:read")
    result = dep(request=mock_request, user=user)

    assert result == user
    assert mock_request.app.state.rbac_service.user_has_permission.call_count == 2


def test_any_permission_none_match(mock_request):
    user = {"sub": str(uuid4()), "is_superuser": False}
    mock_request.app.state.rbac_service.user_has_permission.return_value = False

    dep = require_any_permission("channel:read", "row:read")
    with pytest.raises(ForbiddenError, match="need one of"):
        dep(request=mock_request, user=user)


def test_any_permission_superuser_bypass(mock_request):
    user = {"sub": str(uuid4()), "is_superuser": True}

    dep = require_any_permission("channel:read", "row:read")
    result = dep(request=mock_request, user=user)

    assert result == user
    mock_request.app.state.rbac_service.user_has_permission.assert_not_called()
