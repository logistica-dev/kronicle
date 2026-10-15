"""Clean up leftover perm_test_* groups from old test runs."""

from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.rbac.rbac_setup import KronicleGroup, KronicleRbac

co = Settings().connection_su
if co is None:
    raise RuntimeError("Settings().connection_su is not configured")
kronicle_rbac = KronicleRbac.from_connection_info(co)

group_list = kronicle_rbac.list_groups()
count = 0
for g in group_list:
    if not isinstance(g, KronicleGroup):
        raise TypeError(f"Expected KronicleGroup, got {type(g).__name__}")
    if g.name and g.name.startswith("perm_test_"):
        del_g = kronicle_rbac.delete_group(group_id=g.id)
        if not isinstance(del_g, KronicleGroup):
            raise TypeError(f"Expected KronicleGroup, got {type(del_g).__name__}")
        count += 1
        print(f"Deleted {del_g.name} (id={del_g.id})")

print(f"Done: {count} groups deleted.")
