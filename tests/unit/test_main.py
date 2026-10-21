# tests/unit/test_main.py
import json
from collections.abc import Awaitable, Callable
from typing import cast
from unittest.mock import AsyncMock, MagicMock, mock_open, patch

import pytest
from fastapi import HTTPException
from pydantic import BaseModel, ValidationError
from starlette.responses import Response

from kronicle.auth.pwd.pwd_manager import PasswordManager
from kronicle.errors.error_types import UnauthorizedError
from kronicle.main import KronicleApp
from kronicle.main import app as app_factory


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
    mock_settings.migration.auto = False

    return mock_settings


@pytest.fixture(autouse=True)
def reset_password_manager():
    """Ensure PasswordManager singleton is reset between tests."""
    yield
    PasswordManager.reset()


@pytest.fixture(autouse=True)
def patch_generate_doc():
    """No unit test may write the tracked docs/openapi.json artifact.

    KronicleApp.__init__ calls generate_doc() in a dev env (main.py), which opens
    and overwrites docs/openapi.json. With the mocked settings above that would
    clobber the real spec with the fixture's TestApp/desc/1.0 values.
    """
    with patch.object(KronicleApp, "generate_doc"):
        yield


# ------------------------------
# Tests
# ------------------------------
def test_factory_initialization_calls_services_and_configs(mock_settings):
    """Ensure that settings, password manager, and JWT are initialized on factory creation."""
    with patch("kronicle.main.PasswordManager.initialize") as MockPwdInit:

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
        KronicleApp(mock_settings)
        mock_routes.assert_called_once()


def test_middleware_initialization_called(mock_settings):
    """Ensure init_middleware is called during factory creation."""
    with patch.object(KronicleApp, "init_middleware") as mock_middleware:
        KronicleApp(mock_settings)
        mock_middleware.assert_called_once()


def test_request_console_logger_attached_when_enabled(mock_settings):
    mock_settings.app.should_log_request = True
    with (
        patch.object(KronicleApp, "init_exception_handlers"),
        patch("kronicle.main.attach_request_console_logger") as mock_attach,
    ):
        KronicleApp(mock_settings)
        mock_attach.assert_called_once_with(True)


def test_request_console_logger_not_attached_when_disabled(mock_settings):
    mock_settings.app.should_log_request = False
    with (
        patch.object(KronicleApp, "init_exception_handlers"),
        patch("kronicle.main.attach_request_console_logger") as mock_attach,
    ):
        KronicleApp(mock_settings)
        mock_attach.assert_called_once_with(False)


def test_exception_handlers_initialization_called(mock_settings):
    """Ensure init_exception_handlers is called during factory creation."""
    with patch.object(KronicleApp, "init_exception_handlers") as mock_handlers:
        KronicleApp(mock_settings)
        mock_handlers.assert_called_once()


def test_value_error_handler_is_registered(mock_settings):
    """ValueError must map to the 400 handler, not the generic 500 handler."""
    from kronicle.errors.exception_handlers import value_error_handler

    factory = KronicleApp(mock_settings)
    assert factory.app.exception_handlers.get(ValueError) is value_error_handler


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
        patch("kronicle.main.MigrationOrchestrator") as MockOrchestrator,
    ):

        mock_channel_db = MockChannelDB.initialize.return_value
        mock_channel_db.init_async = AsyncMock()
        mock_channel_db.close = AsyncMock()

        mock_rbac_db = MockRbacDB.return_value
        mock_rbac_db.close = MagicMock()

        factory = KronicleApp(mock_settings)
        app = factory.app

        async with app.router.lifespan_context(app):
            mock_channel_db.init_async.assert_awaited()
            MockOrchestrator.assert_called_once_with(mock_settings.db)
            MockOrchestrator.return_value.validate.assert_called_once()

        mock_channel_db.close.assert_awaited()
        mock_rbac_db.close.assert_called()


@pytest.mark.asyncio
async def test_lifespan_auto_migration_runs_orchestrator_and_seeds_superuser(mock_settings):
    """INIT/auto mode: orchestrator.run(auto_approve=True) + app superuser seeding."""
    mock_settings.migration.auto = True
    with (
        patch("kronicle.main.ChannelDbSession") as MockChannelDB,
        patch("kronicle.main.RbacDbSession") as MockRbacDB,
        patch("kronicle.main.ChannelRepository"),
        patch("kronicle.main.ChannelService"),
        patch("kronicle.main.RbacService"),
        patch("kronicle.main.JWTService"),
        patch("kronicle.main.MigrationOrchestrator") as MockOrchestrator,
        patch("kronicle.main.seed_app_superuser") as mock_seed_su,
        patch("kronicle.main.AppSuperuser") as MockSu,
    ):

        mock_channel_db = MockChannelDB.initialize.return_value
        mock_channel_db.init_async = AsyncMock()
        mock_channel_db.close = AsyncMock()
        mock_rbac_db = MockRbacDB.return_value

        factory = KronicleApp(mock_settings)
        app = factory.app

        async with app.router.lifespan_context(app):
            MockOrchestrator.return_value.run.assert_called_once_with(auto_approve=True)
            MockOrchestrator.return_value.validate.assert_not_called()
            MockSu.from_env.assert_called_once()
            mock_seed_su.assert_called_once_with(mock_rbac_db, su=MockSu.from_env.return_value)

        mock_channel_db.close.assert_awaited()


# ----------------------------------------------------------------------------------------------
# Full-stack request tests (root routes, middleware, error funnel)
# ----------------------------------------------------------------------------------------------
class _IntModel(BaseModel):
    x: int


async def _asgi_call(asgi_app, method: str, path: str):
    """Drive the full ASGI app (middleware stack included) without an HTTP client."""
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }
    messages = []
    state = {"received": False}

    async def receive():
        if not state["received"]:
            state["received"] = True
            return {"type": "http.request", "body": b"", "more_body": False}
        return {"type": "http.disconnect"}

    async def send(message):
        messages.append(message)

    await asgi_app(scope, receive, send)
    status = next(m["status"] for m in messages if m["type"] == "http.response.start")
    body = b"".join(m.get("body", b"") for m in messages if m["type"] == "http.response.body")
    return status, body


@pytest.mark.asyncio
async def test_root_route_returns_app_name(mock_settings):
    factory = KronicleApp(mock_settings)
    status, body = await _asgi_call(factory.app, "GET", "/")
    assert status == 200
    assert json.loads(body) == {"app": "TestApp"}


@pytest.mark.asyncio
async def test_favicon_route_serves_file(mock_settings):
    factory = KronicleApp(mock_settings)
    status, _ = await _asgi_call(factory.app, "GET", "/favicon.ico")
    assert status == 200


@pytest.mark.asyncio
async def test_trailing_slash_is_stripped(mock_settings):
    """The strip_trailing_slash middleware rewrites /health/live/ -> /health/live."""
    factory = KronicleApp(mock_settings)
    status, _ = await _asgi_call(factory.app, "GET", "/health/live/")
    assert status == 200


@pytest.mark.asyncio
async def test_unhandled_exception_returns_500_payload(mock_settings):
    """A generic Exception escapes the inner middlewares and is funnelled to a 500 JSON payload.

    The route's permission dependency raises AttributeError (app.state.rbac_service is
    never initialized in this test), which nothing handles until catch_all_exceptions.
    """
    factory = KronicleApp(mock_settings)
    factory.app.state.allow_anonymous = True
    status, body = await _asgi_call(factory.app, "GET", "/api/v1/channels")
    assert status == 500
    payload = json.loads(body)
    assert payload["error"] == "InternalServerError"


def _catch_all_dispatch(app) -> Callable[[MagicMock, AsyncMock], Awaitable[Response]]:
    """Extract the `catch_all_exceptions` middleware dispatch registered by KronicleApp."""
    dispatch = next(
        m.kwargs["dispatch"]
        for m in app.user_middleware
        if getattr(m.kwargs.get("dispatch"), "__name__", "") == "catch_all_exceptions"
    )
    return cast("Callable[[MagicMock, AsyncMock], Awaitable[Response]]", dispatch)


@pytest.mark.asyncio
async def test_catch_all_middleware_reraises_known_exceptions(mock_settings):
    """HTTPException / KronicleAppError / ValidationError must re-raise (not become a 500 here)."""
    factory = KronicleApp(mock_settings)
    catch_all = _catch_all_dispatch(factory.app)

    with pytest.raises(ValidationError) as caught:
        _IntModel()  # type: ignore[call-arg]
    re_raised = [HTTPException(status_code=400, detail="bad"), UnauthorizedError("nope"), caught.value]

    for exc in re_raised:
        call_next = AsyncMock(side_effect=exc)
        with pytest.raises(type(exc)):
            await catch_all(MagicMock(), call_next)


@pytest.mark.asyncio
async def test_catch_all_middleware_logs_single_line_without_traceback(mock_settings):
    """An exception with no traceback (no user frames to list) is still funnelled to a 500 payload."""

    class _NoTracebackError(RuntimeError):
        @property
        def __traceback__(self):  # type: ignore[assignment]
            return None

    factory = KronicleApp(mock_settings)
    catch_all = _catch_all_dispatch(factory.app)
    request = MagicMock()
    request.method = "GET"
    request.url.path = "/test"
    request.state.request_id = "test-request-id"

    call_next = AsyncMock(side_effect=_NoTracebackError("boom"))
    with patch("kronicle.main.log_e") as mock_log:
        response = await catch_all(request, call_next)

    assert response.status_code == 500
    frameless_logs = [c for c in mock_log.call_args_list if "Traceback (user frames only)" not in str(c)]
    assert frameless_logs, "expected a single-line error log when there are no user frames"


# ----------------------------------------------------------------------------------------------
# Doc generation and app factory
# ----------------------------------------------------------------------------------------------
# Captured before the autouse patch_generate_doc fixture replaces it per-test.
_real_generate_doc = KronicleApp.generate_doc


def test_generate_doc_writes_openapi_spec(mock_settings):
    factory = KronicleApp(mock_settings)
    opened = mock_open()
    with patch("builtins.open", opened), patch("kronicle.main.dump") as mock_dump:
        _real_generate_doc(factory)

    docs_path = opened.call_args[0][0]
    assert docs_path.name == "openapi.json"
    assert docs_path.parent.name == "docs"
    written_spec, file_handle = mock_dump.call_args[0]
    assert written_spec == factory.app.openapi()
    assert file_handle is opened.return_value


def test_app_factory_builds_app_from_settings():
    conf = MagicMock()
    with (
        patch("kronicle.main.KronicleSettings", return_value=conf) as MockSettings,
        patch("kronicle.main.KronicleApp") as MockKronicleApp,
    ):
        MockKronicleApp.return_value.app = "built-app"
        result = app_factory()

    MockSettings.assert_called_once_with()
    MockKronicleApp.assert_called_once_with(conf)
    assert result == "built-app"
