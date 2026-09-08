# tests/integration/health/test_health_check.py

import pytest
import requests
from kronicle_sdk.conf.read_conf import Settings


@pytest.fixture(scope="session")
def base_url():
    co = Settings().connection_su
    assert co
    return co.url


@pytest.mark.integration
def test_version_endpoint_returns_build_info(base_url):
    """GET /health/version is unauthenticated and returns build metadata."""
    resp = requests.get(f"{base_url}/health/version", timeout=5)
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body, dict)
    assert "version" in body
    assert "commit" in body
    assert "date" in body


@pytest.mark.integration
def test_liveness_endpoint(base_url):
    """GET /health/live reports the service is alive (no auth required)."""
    resp = requests.get(f"{base_url}/health/live", timeout=5)
    assert resp.status_code == 200
    assert resp.json() == {"status": "alive"}


@pytest.mark.integration
def test_readiness_endpoint(base_url):
    """GET /health/ready probes the DB and reports readiness."""
    resp = requests.get(f"{base_url}/health/ready", timeout=5)
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body, dict)
    assert "status" in body
