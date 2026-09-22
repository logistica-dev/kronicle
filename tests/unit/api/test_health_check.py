# tests/unit/api/test_health_check.py
import json
from unittest.mock import AsyncMock

from kronicle._build import __commit__
from kronicle.api.health_check import liveness, readiness, version
from kronicle.deps.settings_ini import package_version


def test_liveness():
    assert liveness() == {"status": "alive"}


def test_readiness_ready():
    data_service = AsyncMock()
    data_service.ping.return_value = True
    import asyncio

    resp = asyncio.run(readiness(data_service=data_service))
    assert resp.status_code == 200
    assert json.loads(bytes(resp.body)) == {"status": "ready"}


def test_readiness_not_ready():
    data_service = AsyncMock()
    data_service.ping.return_value = False
    import asyncio

    resp = asyncio.run(readiness(data_service=data_service))
    assert resp.status_code == 503
    assert json.loads(bytes(resp.body)) == {"status": "not_ready"}


def test_readiness_raises_returns_error():
    data_service = AsyncMock()
    data_service.ping.side_effect = RuntimeError("db down")
    import asyncio

    resp = asyncio.run(readiness(data_service=data_service))
    assert resp.status_code == 503
    assert json.loads(bytes(resp.body))["status"] == "not_ready"
    assert json.loads(bytes(resp.body))["error"] == "db down"


def test_version():
    result = version()
    assert result["version"] == package_version()
    assert result["commit"] == __commit__
