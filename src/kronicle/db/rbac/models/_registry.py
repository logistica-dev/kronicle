# kronicle/db/rbac/models/_registry.py
from kronicle.db.rbac.links.group_hierarchy import RbacGroupHierarchy
from kronicle.db.rbac.links.group_roles import RbacGroupRoles
from kronicle.db.rbac.links.rbac_access_profile import ChannelAccessProfile, RowAccessProfile, ZoneAccessProfile
from kronicle.db.rbac.links.rbac_policy import ChannelPolicy, RowPolicy, ZonePolicy
from kronicle.db.rbac.links.user_groups import RbacUserGroups
from kronicle.db.rbac.links.user_roles import RbacUserRoles
from kronicle.db.rbac.models.rbac_entity import RbacEntity
from kronicle.db.rbac.models.rbac_group import RbacGroup
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
    # Group hierarchy
    RbacGroupHierarchy,
    # These should be created afterwards as they link to previous tables
    RbacUserGroups,
    RbacUserRoles,
    RbacGroupRoles,
    RbacSubject,
    # Access profiles (resource <-> role)
    ZoneAccessProfile,
    ChannelAccessProfile,
    RowAccessProfile,
    # Policies (profile <-> subject)
    ZonePolicy,
    ChannelPolicy,
    RowPolicy,
    # This one should be last
    # RbacEvent,
]
