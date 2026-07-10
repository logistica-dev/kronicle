# tests/integration/conftest.py
import os

import pytest


def pytest_collection_modifyitems(config, items):
    run_mode = config.getoption("-m", default="")
    run_all = config.getoption("--run-all")

    for item in items:
        if "tests/integration" in str(item.fspath):
            item.add_marker("integration")

        should_skip = run_mode != "integration" and not run_all

        if "integration" in item.keywords and should_skip:
            item.add_marker(pytest.mark.skip(reason="Integration tests require `-m integration` or `--run-all`"))

    if not (
        os.environ.get("KRONICLE_USR_NAME")
        and os.environ.get("KRONICLE_USR_PASS")
        and os.environ.get("KRONICLE_SU_NAME")
        and os.environ.get("KRONICLE_SU_PASS")
    ):
        print("W [conftest] !!! Kronicle env variables not found, skipping integration tests.")
        skip_missing_env = pytest.mark.skip(reason="Integration tests require server env")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_missing_env)
    else:
        print("I [conftest] Kronicle env variables were found, running integration")
