# kronicle/db/rbac/links/rbac_link.py

from kronicle.db.base.kronicle_link import KronicleLink
from kronicle.db.rbac.models.rbac_entity import RbacEntity


class RbacLink(KronicleLink):

    __abstract__ = True  # Do not create a table for this class itself

    USER_ID = "user_id"
    GROUP_ID = "group_id"
    ROLE_ID = "role_id"
    ACCESS_PROFILE_ID = "access_profile_id"

    ZONE_ID = "zone_id"
    CHANNEL_ID = "channel_id"
    ROW_ID = "row_id"

    @classmethod
    def namespace(cls) -> str:
        return RbacEntity.namespace()
