"""Clean up leftover perm_test_*@kronicle.app users from old test runs."""

from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.rbac.rbac_setup import KronicleRbac

co = Settings().connection_su
assert co
kronicle_rbac = KronicleRbac.from_connection_info(co)

usr_list = kronicle_rbac.get_all_users(include_inactive=True)
count = 0
for u in usr_list:
    if u.email.startswith("perm_test_") and u.email.endswith("@kronicle.app"):
        del_u = kronicle_rbac.remove_user(u)
        count += 1
        print(f"Deleted {del_u.email} (id={del_u.id})")

print(f"Done: {count} users deleted.")
