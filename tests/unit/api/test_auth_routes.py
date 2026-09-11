# tests/unit/api/test_auth_routes.py
from unittest.mock import MagicMock

from kronicle.api.auth_routes import login, update_password


def test_login_returns_bearer_token():
    auth = MagicMock()
    auth.login.return_value = "the-token"
    creds = MagicMock()
    result = login(creds=creds, auth=auth)
    auth.login.assert_called_once_with(creds)
    assert result == {"access_token": "the-token", "token_type": "bearer"}


def test_update_password_returns_bearer_token():
    auth = MagicMock()
    auth.change_password.return_value = "new-token"
    creds = MagicMock()
    result = update_password(creds=creds, auth=auth)
    auth.change_password.assert_called_once_with(creds)
    assert result == {"access_token": "new-token", "token_type": "bearer"}
