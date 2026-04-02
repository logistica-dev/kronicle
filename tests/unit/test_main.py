# tests/unit/main/test_kronicle_app_factory_unit.py
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kronicle.auth.pwd.pwd_manager import PasswordManager
from kronicle.main import KronicleApp


def teardown_function():
    PasswordManager._instance = None


# ------------------------------
# Test: Initialization calls services and configs
# ------------------------------
def test_factory_initialization_calls_settings_and_services():
    """Ensure that settings, password manager, and JWT are initialized on factory creation."""
    with (
        patch("kronicle.main.Settings") as MockSettings,
        patch("kronicle.main.PasswordManager.initialize") as MockPwdInit,
        patch("kronicle.main.JWTService") as MockJWT,
    ):
        # Create a proper mock for Settings().app
        mock_app = MagicMock()
        mock_app.name = "TestApp"
        mock_app.version = "1.0"
        mock_app.description = "desc"
        mock_app.openapi_url = "/openapi.json"

        # Other top-level settings
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

        MockSettings.return_value = mock_settings

        # This should now work without TypeError
        factory = KronicleApp()
        assert factory.app.title == "TestApp"
        assert factory.app.version == "1.0"
        assert factory.app.openapi_url == "/openapi.json"
        # PasswordManager.initialize called once
        MockPwdInit.assert_called_once()


# ------------------------------
# Test: Internal methods exist
# ------------------------------
def test_app_has_lifespan_method():
    """Ensure the lifespan method exists on the factory."""
    factory = KronicleApp()
    assert hasattr(factory, "lifespan")
    assert callable(factory.lifespan)


def test_routes_initialization_called(monkeypatch):
    """Ensure init_routes is called during factory creation."""
    with patch.object(KronicleApp, "init_routes") as mock_routes:
        factory = KronicleApp()
        mock_routes.assert_called_once()


def test_middleware_initialization_called(monkeypatch):
    """Ensure init_middleware is called during factory creation."""
    with patch.object(KronicleApp, "init_middleware") as mock_middleware:
        factory = KronicleApp()
        mock_middleware.assert_called_once()


def test_exception_handlers_initialization_called(monkeypatch):
    """Ensure init_exception_handlers is called during factory creation."""
    with patch.object(KronicleApp, "init_exception_handlers") as mock_handlers:
        factory = KronicleApp()
        mock_handlers.assert_called_once()


# ------------------------------
# Test: Lifespan startup/shutdown (unit only)
# ------------------------------
@pytest.mark.asyncio
async def test_lifespan_calls_db_methods():
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

        factory = KronicleApp()
        app = factory.app

        # Run lifespan context manually (unit-level, async)
        async with app.router.lifespan_context(app):
            mock_channel_db.init_async.assert_awaited()
            mock_rbac_db.validate_tables.assert_called()

        mock_channel_db.close.assert_awaited()
        mock_rbac_db.close.assert_called()
