# tests/integration/conftest.py
import os

import pytest

pytestmark = pytest.mark.integration


def pytest_collection_modifyitems(config, items):
    # skip integration tests if KRONICLE_URL not set
    if not (os.environ.get("KRONICLE_USR_NAME") and os.environ.get("KRONICLE_USR_PASS")):
        skip_integration = pytest.mark.skip(reason="Integration tests require server env")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)
