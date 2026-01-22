# kronicle/db/rbac/models/__init__.py
from kronicle.db.rbac.associations.group_roles import RbacGroupRoles
from kronicle.db.rbac.associations.user_groups import RbacUserGroups
from kronicle.db.rbac.associations.user_roles import RbacUserRoles
from kronicle.db.rbac.models.rbac_access_profile import ChannelAccessProfile, ZoneAccessProfile
from kronicle.db.rbac.models.rbac_entity import RbacEntity
from kronicle.db.rbac.models.rbac_group import RbacGroup
from kronicle.db.rbac.models.rbac_policy import ChannelPolicy, ZonePolicy
from kronicle.db.rbac.models.rbac_role import RbacRole
from kronicle.db.rbac.models.rbac_subject import RbacSubject
from kronicle.db.rbac.models.rbac_user import RbacUser

# from kronicle.db.rbac.models.rbac_event import RbacEvent

# Centralized list of all RBAC tables
RBAC_NAMESPACE = RbacEntity.namespace()

# Centralized list of all RBAC tables
ALL_RBAC_TABLES = [
    RbacUser,
    # User then Group
    RbacGroup,
    RbacRole,
    # These should be created afterwards as they link to previous tables
    RbacUserGroups,
    RbacUserRoles,
    RbacGroupRoles,
    RbacSubject,
    ChannelAccessProfile,
    ZoneAccessProfile,
    # Then these should be created afterwards
    ChannelPolicy,
    ZonePolicy,
    # This one should be last
    # RbacEvent,
]
