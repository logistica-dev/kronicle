"""Clean up leftover perm_test_* groups from old test runs."""

from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.rbac.rbac_setup import KronicleGroup, KronicleRbac

co = Settings().connection_su
assert co
kronicle_rbac = KronicleRbac.from_connection_info(co)

group_list = kronicle_rbac.get_all_groups()
count = 0
for g in group_list:
    assert g is KronicleGroup
    if g.name and g.name.startswith("perm_test_"):
        del_g = kronicle_rbac.delete_group(g.id)
        assert del_g is KronicleGroup
        count += 1
        print(f"Deleted {del_g.name} (id={del_g.id})")

print(f"Done: {count} groups deleted.")
