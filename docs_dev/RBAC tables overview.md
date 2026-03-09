# RBAC Schema Overview

- API layer (**FastAPI**) → transport + validation
- Application layer (**RbacService**) → use-case orchestration, input/output mapping
- Domain layer (**RbacManager**) → business rules
- Persistence layer (SQLAlchemy models / repositories) → pure data access

## Core Tables

| Class            | Description                                                                           |
| ---------------- | ------------------------------------------------------------------------------------- |
| **Channel**      | Represents a data channel within a Zone. Core resource.                               |
| **Zone**         | Represents a project or workspace. Can contain multiple Channels. Core resource.      |
| **CoreResource** | View combining all core resources (`Zone` + `Channel`) for easier access and queries. |

## RBAC Tables

| Class              | Description                                                                                                                                       |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| **RbacUser**       | Represents a user in the system. Holds credentials, active status, and links to groups and roles. Provides DB methods to fetch users by login.    |
| **RbacGroup**      | Represents a group of users. Can have hierarchical structure via `KronicleHierarchyMixin`. Can retrieve all users including children recursively. |
| **RbacRole**       | Represents a set of permissions (actions) that can be granted on a resource. Can be assigned to users or groups.                                  |
| **RbacUserGroups** | Association table mapping `RbacUser` → `RbacGroup`. Provides methods to get all groups for a user or all users in a group.                        |
| **RbacUserRoles**  | Association table mapping `RbacUser` → `RbacRole`. Provides methods to get all roles for a user or all users with a role.                         |
| **RbacGroupRoles** | Association table mapping `RbacGroup` → `RbacRole`. Provides methods to get all roles for a group or all groups with a role.                      |
| **RbacSubject**    | View unifying all subjects (`RbacUser` + `RbacGroup`) to simplify policy assignment and lookups.                                                  |
| **AccessProfile**  | Represents a pre-defined set of permissions on a resource (like ZoneReader, ZoneWriter, ZoneAdmin). Can be assigned to subjects via policies.     |
| **RbacPolicy**     | Associates a subject (`RbacUser` or `RbacGroup`) with an `AccessProfile` for a specific resource. Determines effective permissions.               |
| **RbacEvent**      | (Optional, created last) Stores historical RBAC events or audits for tracking changes to users, groups, roles, policies, etc.                     |
