# tests/integration/conftest.py
import os

import pytest

pytestmark = pytest.mark.integration


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "tests/integration" in str(item.fspath):
            item.add_marker("integration")
        # skip integration tests unless explicitly running with -m integration
        if "integration" in item.keywords and config.getoption("-m") != "integration":
            item.add_marker(pytest.mark.skip(reason="Integration tests require -m integration"))

    # skip integration tests if KRONICLE_URL not set
    if not (os.environ.get("KRONICLE_USR_NAME") and os.environ.get("KRONICLE_USR_PASS")):
        print("W [conftest] !!! Kronicle env variables not found, skipping integration tests.")
        skip_integration = pytest.mark.skip(reason="Integration tests require server env")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)
    else:
        print("I [conftest] Kronicle env variables were found, running integration")
