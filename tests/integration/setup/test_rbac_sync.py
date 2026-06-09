# tests/integration/setup/test_rbac_sync.py

import pytest
from kronicle_sdk.utils.log import log_d


@pytest.mark.integration
def test_sync_core_channels(kronicle_rbac_setup, test_channel):
    """Sync data channels to CoreChannels and verify the result."""
    here = "setup_sync"
    result = kronicle_rbac_setup.sync_core_channels()
    log_d(here, "Sync result", result)
    assert isinstance(result, dict)
    assert "detail" in result
    assert "total_data_channels" in result
    assert "created_core_channels" in result
    assert "default_zone_id" in result
