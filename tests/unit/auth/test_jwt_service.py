# tests/unit/auth/test_jwt_service.py
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from jose import jwt

from kronicle.auth.jwt_service import JWTService
from kronicle.errors.error_types import UnauthorizedError


@pytest.fixture
def jwt_conf():
    conf = MagicMock()
    conf.get_secret.return_value = "test-secret-key-12345"
    conf.algorithm = "HS256"
    conf.expiration_minutes = 30
    return conf


@pytest.fixture
def service(jwt_conf):
    return JWTService(jwt_conf=jwt_conf)


@pytest.fixture
def mock_user():
    user = MagicMock()
    user.id = uuid4()
    user.is_su = False
    return user


class TestInit:
    def test_initializes_with_config(self, jwt_conf):
        svc = JWTService(jwt_conf=jwt_conf)
        assert svc._secret == "test-secret-key-12345"
        assert svc._algo == "HS256"
        assert svc._exp_minutes == 30

    def test_stores_config_values(self, jwt_conf):
        jwt_conf.get_secret.return_value = "another-secret"
        jwt_conf.algorithm = "HS512"
        jwt_conf.expiration_minutes = 60
        svc = JWTService(jwt_conf=jwt_conf)
        assert svc._secret == "another-secret"
        assert svc._algo == "HS512"
        assert svc._exp_minutes == 60


class TestGetPayloadFromOutUser:
    def test_regular_user_has_no_superuser_flag(self, service, mock_user):
        mock_user.is_su = False
        payload = service._get_payload_from_out_user(mock_user)
        assert payload["sub"] == str(mock_user.id)
        assert "exp" in payload
        assert isinstance(payload["exp"], int)
        assert "is_superuser" not in payload

    def test_superuser_has_flag(self, service, mock_user):
        mock_user.is_su = True
        payload = service._get_payload_from_out_user(mock_user)
        assert payload["sub"] == str(mock_user.id)
        assert payload["is_superuser"] is True


class TestCreateAccessToken:
    def test_returns_encoded_jwt(self, service, mock_user):
        token = service.create_access_token(mock_user)
        assert isinstance(token, str)
        assert len(token.split(".")) == 3

    @patch("kronicle.auth.jwt_service.jwt.encode")
    def test_calls_encode_with_correct_args(self, mock_encode, service, mock_user):
        mock_encode.return_value = "fake-token"
        token = service.create_access_token(mock_user)
        assert token == "fake-token"
        args, kwargs = mock_encode.call_args
        payload = args[0]
        assert payload["sub"] == str(mock_user.id)
        assert "exp" in payload
        assert args[1] == "test-secret-key-12345"
        assert kwargs["algorithm"] == "HS256"


class TestDecodeToken:
    def test_valid_token_decodes_correctly(self, service):
        token = jwt.encode({"sub": "123"}, "test-secret-key-12345", algorithm="HS256")
        result = service.decode_token(token)
        assert result["sub"] == "123"

    def test_decode_with_full_payload(self, service, mock_user):
        token = service.create_access_token(mock_user)
        result = service.decode_token(token)
        assert result["sub"] == str(mock_user.id)

    def test_invalid_token_raises_unauthorized(self, service):
        with pytest.raises(UnauthorizedError, match="Invalid token"):
            service.decode_token("invalid.jwt.token")

    def test_wrong_secret_raises_unauthorized(self, service):
        token = jwt.encode({"sub": "123"}, "wrong-secret", algorithm="HS256")
        with pytest.raises(UnauthorizedError, match="Invalid token"):
            service.decode_token(token)

    def test_tampered_token_raises_unauthorized(self, service):
        token = jwt.encode({"sub": "123"}, "test-secret-key-12345", algorithm="HS256")
        parts = token.split(".")
        tampered = parts[0] + "." + parts[1] + ".invalidsignature"
        with pytest.raises(UnauthorizedError, match="Invalid token"):
            service.decode_token(tampered)


class TestVerifyToken:
    def test_valid_token_returns_true(self, service):
        token = jwt.encode({"sub": "123"}, "test-secret-key-12345", algorithm="HS256")
        assert service.verify_token(token) is True

    def test_invalid_token_returns_false(self, service):
        assert service.verify_token("invalid.jwt.token") is False

    def test_tampered_token_returns_false(self, service):
        token = jwt.encode({"sub": "123"}, "test-secret-key-12345", algorithm="HS256")
        parts = token.split(".")
        tampered = parts[0] + "." + parts[1] + ".badsig"
        assert service.verify_token(tampered) is False

    def test_wrong_secret_returns_false(self, service):
        token = jwt.encode({"sub": "123"}, "wrong-secret", algorithm="HS256")
        assert service.verify_token(token) is False
