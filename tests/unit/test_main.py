# tests/unit/main/test_kronicle_app_factory_unit.py
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kronicle.auth.pwd.pwd_manager import PasswordManager
from kronicle.main import KronicleApp


# ------------------------------
# Fixtures
# ------------------------------
@pytest.fixture
def mock_settings():
    """Return a fully mocked Settings object."""
    mock_app = MagicMock()
    mock_app.name = "TestApp"
    mock_app.version = "1.0"
    mock_app.description = "desc"
    mock_app.openapi_url = "/openapi.json"

    mock_settings = MagicMock()
    mock_settings.app = mock_app
    mock_settings.auth = MagicMock(
        pwd_min_length=8,
        pwd_require_uppercase=True,
        pwd_require_lowercase=True,
        pwd_require_digits=True,
        pwd_require_special=True,
        pwd_special_chars="!@#$",
    )
    mock_settings.jwt = MagicMock()
    mock_settings.server = MagicMock(host="localhost", port=8000)
    mock_settings.api_version = "v1"
    mock_settings.is_prod_env = False
    mock_settings.is_dev_env = True

    return mock_settings


@pytest.fixture(autouse=True)
def reset_password_manager():
    """Ensure PasswordManager singleton is reset between tests."""
    yield
    PasswordManager._instance = None


# ------------------------------
# Tests
# ------------------------------
def test_factory_initialization_calls_services_and_configs(mock_settings):
    """Ensure that settings, password manager, and JWT are initialized on factory creation."""
    with patch("kronicle.main.PasswordManager.initialize") as MockPwdInit, patch("kronicle.main.JWTService") as MockJWT:

        factory = KronicleApp(mock_settings)

        # App properties
        assert factory.app.title == "TestApp"
        assert factory.app.version == "1.0"
        assert factory.app.openapi_url == "/openapi.json"

        # PasswordManager.initialize called
        MockPwdInit.assert_called_once()


def test_app_has_lifespan_method(mock_settings):
    """Ensure the lifespan method exists on the factory."""
    factory = KronicleApp(mock_settings)
    assert hasattr(factory, "lifespan")
    assert callable(factory.lifespan)


def test_routes_initialization_called(mock_settings):
    """Ensure init_routes is called during factory creation."""
    with patch.object(KronicleApp, "init_routes") as mock_routes:
        factory = KronicleApp(mock_settings)
        mock_routes.assert_called_once()


def test_middleware_initialization_called(mock_settings):
    """Ensure init_middleware is called during factory creation."""
    with patch.object(KronicleApp, "init_middleware") as mock_middleware:
        factory = KronicleApp(mock_settings)
        mock_middleware.assert_called_once()


def test_exception_handlers_initialization_called(mock_settings):
    """Ensure init_exception_handlers is called during factory creation."""
    with patch.object(KronicleApp, "init_exception_handlers") as mock_handlers:
        factory = KronicleApp(mock_settings)
        mock_handlers.assert_called_once()


# ------------------------------
# Lifespan tests (async)
# ------------------------------
@pytest.mark.asyncio
async def test_lifespan_calls_db_methods(mock_settings):
    """Ensure lifespan calls init and close on mocked DB sessions."""
    with (
        patch("kronicle.main.ChannelDbSession") as MockChannelDB,
        patch("kronicle.main.RbacDbSession") as MockRbacDB,
        patch("kronicle.main.ChannelRepository"),
        patch("kronicle.main.ChannelService"),
        patch("kronicle.main.RbacService"),
        patch("kronicle.main.JWTService"),
    ):

        mock_channel_db = MockChannelDB.return_value
        mock_channel_db.init_async = AsyncMock()
        mock_channel_db.close = AsyncMock()

        mock_rbac_db = MockRbacDB.return_value
        mock_rbac_db.validate_tables = MagicMock()
        mock_rbac_db.close = MagicMock()

        factory = KronicleApp(mock_settings)
        app = factory.app

        async with app.router.lifespan_context(app):
            mock_channel_db.init_async.assert_awaited()
            mock_rbac_db.validate_tables.assert_called()

        mock_channel_db.close.assert_awaited()
        mock_rbac_db.close.assert_called()
