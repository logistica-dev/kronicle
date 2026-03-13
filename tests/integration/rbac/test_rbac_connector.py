# tests/integration/rbac/rbac_connector.py

from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.rbac.rbac_connector import KronicleRbacConnector
from kronicle_sdk.models.rbac.kronicle_user import KronicleUser
from kronicle_sdk.utils.log import log_d, log_w

if __name__ == "__main__":
    here = "abstract Kronicle connector"
    log_d(here)
    co = Settings().connection
    kronicle_rbac = KronicleRbacConnector(co.url, co.usr, co.pwd)
    usr_list = kronicle_rbac.get_all_users()
    [log_d(here, usr) for usr in usr_list]
    usr1 = usr_list[0]
    log_d(here, "usr1 obj", usr1)
    log_d(here, "usr1.email", usr1.email)

    log_d("get by email", kronicle_rbac.get_user_by(email=usr1.email))
    log_d("get by name", kronicle_rbac.get_user_by(name=usr1.name))
    log_d("get fake name", kronicle_rbac.get_user_by(name=f"{usr1.name}3"))

    usr2 = KronicleUser(
        email="dave@toto.fr",
        name="Dave",
        orcid="1234-5678-9101",
        full_name="Dave Bond",
        password="Wonderful_Secrets_123403657",
    )
    try:
        res = kronicle_rbac.create_user(usr2)
        log_d(here, "Created", res)
    except Exception as e:
        log_w(here, e)

    usr2 = KronicleUser(
        email="dave@toto.fr",
        name="Dave2",
        orcid="1234-5678-9102",
        full_name="Dave Bond II",
        # password="Wonderful_Secrets_123403657",
    )
    res = kronicle_rbac.patch_user(usr2)
    log_d(here, "Updated", res)
    res = kronicle_rbac.delete_user(usr2)
    log_d(here, "Deleted", res)
