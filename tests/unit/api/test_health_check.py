# tests/unit/api/test_health_check.py
from unittest.mock import AsyncMock

from kronicle._build import __commit__, __version__
from kronicle.api.health_check import liveness, readiness, version


def test_liveness():
    assert liveness() == {"status": "alive"}


def test_readiness_ready():
    data_service = AsyncMock()
    data_service.ping.return_value = True
    result = readiness(data_service=data_service)
    # readiness is an async function
    import asyncio

    assert asyncio.run(result) == {"status": "ready"}


def test_readiness_not_ready():
    data_service = AsyncMock()
    data_service.ping.return_value = False
    import asyncio

    assert asyncio.run(readiness(data_service=data_service)) == {"status": "not_ready"}


def test_readiness_raises_returns_error():
    data_service = AsyncMock()
    data_service.ping.side_effect = RuntimeError("db down")
    import asyncio

    result = asyncio.run(readiness(data_service=data_service))
    assert result["status"] == "not_ready"
    assert result["error"] == "db down"


def test_version():
    result = version()
    assert result["version"] == __version__
    assert result["commit"] == __commit__
