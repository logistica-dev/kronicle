# kronicle/schemas/permissions/permission_sets.py

from kronicle.schemas.permissions.permission import PermStr

PERMISSION_SET_SUPER_ADMIN = [
    PermStr.USER_CREATE,
    PermStr.USER_READ,
    PermStr.USER_UPDATE,
    PermStr.USER_DELETE,
    PermStr.ROLE_CREATE,
    PermStr.ROLE_READ,
    PermStr.ROLE_UPDATE,
    PermStr.ROLE_DELETE,
    PermStr.ROLE_ASSIGN,
    PermStr.GROUP_CREATE,
    PermStr.GROUP_READ,
    PermStr.GROUP_UPDATE,
    PermStr.GROUP_DELETE,
    PermStr.GROUP_ASSIGN,
    PermStr.POLICY_CREATE,
    PermStr.POLICY_READ,
    PermStr.POLICY_DELETE,
    PermStr.ZONE_CREATE,
    PermStr.ZONE_READ,
    PermStr.ZONE_UPDATE,
    PermStr.ZONE_DELETE,
    PermStr.CHANNEL_CREATE,
    PermStr.CHANNEL_READ,
    PermStr.CHANNEL_UPDATE,
    PermStr.CHANNEL_DELETE,
    PermStr.CHANNEL_SYNC,
    PermStr.ROW_READ,
    PermStr.ROW_CREATE,
    PermStr.ROW_DELETE,
    PermStr.RBAC_ACCESS,
    PermStr.DATA_ACCESS,
    PermStr.SETUP_ACCESS,
]

PERMISSION_SET_RBAC_ADMIN = [
    PermStr.RBAC_ACCESS,
    PermStr.USER_CREATE,
    PermStr.USER_READ,
    PermStr.USER_UPDATE,
    PermStr.USER_DELETE,
    PermStr.ROLE_CREATE,
    PermStr.ROLE_READ,
    PermStr.ROLE_UPDATE,
    PermStr.ROLE_DELETE,
    PermStr.ROLE_ASSIGN,
    PermStr.GROUP_CREATE,
    PermStr.GROUP_READ,
    PermStr.GROUP_UPDATE,
    PermStr.GROUP_DELETE,
    PermStr.GROUP_ASSIGN,
    PermStr.POLICY_CREATE,
    PermStr.POLICY_READ,
    PermStr.POLICY_DELETE,
]

PERMISSION_SET_DATA_READER = [
    PermStr.CHANNEL_READ,
    PermStr.ROW_READ,
]

PERMISSION_SET_DATA_WRITER = [
    PermStr.DATA_ACCESS,
    PermStr.CHANNEL_READ,
    PermStr.ROW_CREATE,
]

PERMISSION_SET_CHANNEL_ADMIN = [
    PermStr.SETUP_ACCESS,
    PermStr.CHANNEL_CREATE,
    PermStr.CHANNEL_READ,
    PermStr.CHANNEL_UPDATE,
    PermStr.CHANNEL_DELETE,
    PermStr.ROW_DELETE,
]

PERMISSION_SET_ZONE_ADMIN = [
    PermStr.ZONE_CREATE,
    PermStr.ZONE_READ,
    PermStr.ZONE_UPDATE,
    PermStr.ZONE_DELETE,
]

PERMISSION_SET_AUDITOR = [
    PermStr.RBAC_ACCESS,
    PermStr.USER_READ,
    PermStr.ROLE_READ,
    PermStr.GROUP_READ,
    PermStr.POLICY_READ,
    PermStr.ZONE_READ,
    PermStr.CHANNEL_READ,
    PermStr.ROW_READ,
]


DEFAULT_ROLES: list[dict] = [
    {
        "name": "super_admin",
        "description": "Full access to all resources and subsystems",
        "permissions": PERMISSION_SET_SUPER_ADMIN,
    },
    {
        "name": "rbac_admin",
        "description": "Manage users, roles, groups, and policies",
        "permissions": PERMISSION_SET_RBAC_ADMIN,
    },
    {
        "name": "data_reader",
        "description": "Read data from channels and rows",
        "permissions": PERMISSION_SET_DATA_READER,
    },
    {"name": "data_writer", "description": "Write data to channels", "permissions": PERMISSION_SET_DATA_WRITER},
    {
        "name": "channel_admin",
        "description": "Create, update, and delete channels",
        "permissions": PERMISSION_SET_CHANNEL_ADMIN,
    },
    {"name": "zone_admin", "description": "Create, update, and delete zones", "permissions": PERMISSION_SET_ZONE_ADMIN},
    {"name": "auditor", "description": "Read-only access to all resources", "permissions": PERMISSION_SET_AUDITOR},
]
