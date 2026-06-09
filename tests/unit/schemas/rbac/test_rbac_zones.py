# tests/unit/schemas/rbac/test_rbac_zones.py

import pytest
from kronicle_sdk.models.rbac.kronicle_zone import KronicleZone
from kronicle_sdk.utils.log import log_d
from kronicle_sdk.utils.str_utils import tiny_id


@pytest.fixture(scope="module")
def test_zone(kronicle_rbac_setup):
    tag = tiny_id()
    zone = KronicleZone(name=f"test_zone_{tag}")
    created = kronicle_rbac_setup.create_zone(zone)
    yield created
    try:
        kronicle_rbac_setup.delete_zone(created.id)
    except Exception:
        pass


@pytest.mark.integration
def test_list_zones(kronicle_rbac_setup, test_zone):
    here = "setup_zones"
    zones = kronicle_rbac_setup.get_zones()
    log_d(here, f"Number of zones: {len(zones)}")
    assert isinstance(zones, list)
    assert len(zones) > 0
    assert test_zone.name in {z.name for z in zones}


@pytest.mark.integration
def test_get_zone(kronicle_rbac_setup, test_zone):
    zone_id = test_zone.id
    zone = kronicle_rbac_setup.get_zone(zone_id)
    assert zone is not None
    assert zone.id == zone_id
    assert zone.name == test_zone.name


@pytest.mark.integration
def test_create_zone(kronicle_rbac_setup):
    here = "setup_zones"
    tag = tiny_id()
    zone = KronicleZone(name=f"zone_{tag}")
    created = kronicle_rbac_setup.create_zone(zone)
    log_d(here, "Created", created)
    assert created is not None
    assert isinstance(created, KronicleZone)
    assert created.name == zone.name
    kronicle_rbac_setup.delete_zone(created.id)


@pytest.mark.integration
def test_delete_zone(kronicle_rbac_setup):
    tag = tiny_id()
    zone = KronicleZone(name=f"del_zone_{tag}")
    created = kronicle_rbac_setup.create_zone(zone)
    deleted = kronicle_rbac_setup.delete_zone(created.id)
    assert deleted is not None
    gone = kronicle_rbac_setup.get_zone(created.id)
    assert gone is None
