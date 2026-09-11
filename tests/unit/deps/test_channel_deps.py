# tests/unit/deps/test_channel_deps.py
from unittest.mock import MagicMock

from kronicle.deps.channel_deps import channel_service


def test_channel_service_returns_service_from_app_state():
    request = MagicMock()
    request.app.state.channel_service = "the-service"
    assert channel_service(request) == "the-service"
