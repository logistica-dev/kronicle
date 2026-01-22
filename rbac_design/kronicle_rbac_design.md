# Role-Based Access Control strategy in Kronicle

_This document defines the architecture, policy logic, and implementation guidelines for Role-Based Access Control (RBAC) in Kronicle._

## Definitions

| Term             | Definition                                                                                                                                                             | Kronicle Examples                                                                                                                      |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| **User**         | An individual identified entity (human or bot) identity.                                                                                                               | `alice@kronicle`, `bob@kronicle`                                                                                                       |
| **Group**        | A collection of **Users**.                                                                                                                                             | `data-science-team`, `admin-team`                                                                                                      |
| **Resource**     | An individual data object or entity.<br>Usually a Kronicle **Channel**, but granularity goes to **Channel Metadata**, or the set of **Channel Rows**.                  | Channel: `channel:climate-data`<br>Metadata: `channel:climate-data/metadata/owner`<br>Row: `channel:climate-data/rows/2024-01-01`      |
| **Zone**         | A set of **Resources** (e.g., all channels in a project).<br>It can also be seen as a logical boundary where permissions are defined preemtively for future resources. | `project:climate-analysis`, `team:data-science`                                                                                        |
| **Action**       | The type of operation being attempted.                                                                                                                                 | `read`, `create`, `update`, `delete`, `manage`                                                                                         |
| **Target**       | An entity that actions are performed on.<br>Can be a **Resource** or **Zone**.                                                                                         | Channel: `channel:climate-data`<br>Channel Metadata: `channel:climate-data/metadata/owner`<br>Channel Row: `channel:climate-data/rows` |
| **Subject**      | An entity that can perform actions. <br>Can be a **User** or a **Group**.                                                                                              | User: `alice@kronicle`<br>Group: `data-science-team`<br>Bot: `monitoring-bot`                                                          |
| **Permission**   | Allow a specific action to be performed on a **Target**.<br>They are inherited from **UserGroups**, but **Users** can have extra **Permissions**.                      | `{ action:'read', target: 'channel:climate-data',  }`<br>`write:metadata`, `manage:bot`, `view:logs`                                   |
| **Restriction**  | Deny a specific action to be performed on a **Target**.<br>They are inherited from **UserGroups**, and **Group** restrictions override **User** **Permissions**.       | `read:channel`, `write:metadata`, `delete:row`, `manage:bot`, `view:logs`                                                              |
| **Role**         | A set of **Permissions** and **Restrictions**.                                                                                                                         | `reader`, `writer`, `admin`, `metadata-editor`, `row-deleter`                                                                          |
| **TargetedRole** | A **Role** for a specific type of **Target**.                                                                                                                          | `reader`, `writer`, `admin`, `metadata-editor`, `row-deleter`                                                                          |
| **Policy**       | A rule that grants a **Subject** a **Role** for a **Target**.                                                                                                          | "The `monitoring-bot` has the `writer` role for `channel:climate-data`."                                                               |
| **Delegation**   | A temporary **Policy**, i.e. a **TargetedRole** assignment from one **User to** another, with optional time boundaries.                                                | "Alice delegates temporary-writer role to service-account-1 for channel:climate-data."                                                 |

## Core Principles

- **Deny by Default:** Access is rejected unless an explicit "allow" policy exists.
- **Defense in Depth:** Security is enforced at both the Network layer (via route prefixes) and the Application layer (via RBAC/Casbin).
- **Separated Management:** Resource administration (`/setup`) is decoupled from identity management (`/rbac`).
- **Data Sovereignty:** Zones enforce hard boundaries for multi-tenant or multi-project isolation.
- **Auditability:** Every RBAC change, delegation, and default role application is logged.

## Route-Level “Security Lanes”

To support network-level firewall policies and infrastructure isolation, Kronicle splits functionality into four distinct prefixes. The route itself determines the available actions.

| Route Prefix | Architectural Intent | Allowed Actions    | Firewall Recommendation                                       |
| :----------- | :------------------- | :----------------- | :------------------------------------------------------------ |
| `/auth/v1`   | **Authentication**   | `post`             | Opened to the public for token issuance, not affected by RBAC |
| `/api/v1`    | **Consumption Lane** | `read`             | Open to internal apps/dashboards.                             |
| `/data/v1`   | **Ingestion Lane**   | `create`           | Restricted to IoT/Sensor IP ranges.                           |
| `/setup/v1`  | **Resource Admin**   | `update`, `delete` | Restricted to Admin VPN/Subnet.                               |
| `/rbac/v1`   | **Identity Admin**   | `manage`           | Highly restricted to Security Ops.                            |

---

## Roles

### Global Roles

These roles have system-wide scope and can manage identity, Zones, and other global Resources.

| Role          | Scope  | Description                                                | Permissions / Actions                                                               |
| ------------- | ------ | ---------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| **Admin**     | Global | Can manage identity and Resources system-wide.             | Create/update/delete Users, Groups, Zones, assign Roles globally or per Zone.       |
| **SuperUser** | Global | Full access to all Resources and administrative functions. | All permissions on all Resources; create/update/delete any Zone, User, Group, Role. |

### Zone-Bound Roles

These roles are bound to a specific Zone or Resource. They apply automatically to all resources inside the Zone unless overridden.

| Role          | Scope         | Description                                           | Permissions / Actions                                                                                    |
| ------------- | ------------- | ----------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| **Reader**    | Zone-specific | Read-only access to Resources in the Zone.            | Read metadata and rows.                                                                                  |
| **Writer**    | Zone-specific | Can create and modify Resources in the Zone.          | Append rows, update metadata, create Channels; may delegate read/write permissions.                      |
| **DataAdmin** | Zone-specific | Full control over Resources in the Zone.              | Delete or modify Channels, Rows, Metadata; assign permissions to other Users (except delegation rights). |
| **ZoneAdmin** | Zone-specific | Manages Users/Groups and default roles within a Zone. | Assign Users/Groups to the Zone, set default Roles for resources, delegate allowed permissions.          |

## **Default Role Propagation:** Any Resource created in a Zone automatically inherits the Zone’s default Roles.

## Role Inheritance & Delegation Rules

1. **Global overrides Zone-bound:**
   - SuperUser/Admin permissions override any Zone-level role.

2. **ZoneAdmin hierarchy:**
   - ZoneAdmin can assign Zone-specific roles (Reader, Writer, DataAdmin) to Users or Groups within the Zone.
   - ZoneAdmin cannot assign global roles like SuperUser or Admin.

3. **Delegation constraints:**
   - Users may only delegate roles/permissions they themselves hold.
   - Delegation may be time-bound (start/end timestamps).
   - ZoneAdmin cannot delegate ZoneAdmin role unless explicitly allowed by a SuperUser.

4. **Automatic default role propagation:**
   - Any new Resource created within a Zone inherits the Zone’s default roles automatically.
   - Default roles can be overridden for specific Resources by Admins or ZoneAdmins.

5. **Conflict resolution:**
   - Restriction rules always override permission rules.
   - User-level policies override Group-level policies.
   - Resource-level policies override Zone-level policies.

### Example Role Assignment Scenarios

| Scenario                                   | Action / Role Assignment                                                                       |
| ------------------------------------------ | ---------------------------------------------------------------------------------------------- |
| SuperUser creates a new Zone               | Assign SuperUser/Admin to manage the Zone globally.                                            |
| Admin assigns a ZoneAdmin to a Zone        | ZoneAdmin can manage Users/Groups and default roles within that Zone.                          |
| ZoneAdmin adds a Writer to a Zone          | Writer can create and modify Channels; inherits default role for future resources in the Zone. |
| Writer delegates Reader role to a teammate | Allowed only for permissions they themselves hold; cannot exceed own access.                   |
| SuperUser overrides Zone default roles     | Can change default roles for any Zone, globally affecting new Resources.                       |

---

## Zones as Domains

A **Zone** acts as a Casbin Domain.

- **Preemptive Rights:** Assigning a Role at the Zone level automatically applies to new Resources created inside.
- **Isolation:** Subjects need explicit assignment to interact with Resources inside a Zone.
- **Resource Binding:** Every Resource must reference its `zone_id`.

---

## Policy Types

| Policy Type        | Description                                       | Example                                                                                              |
| ------------------ | ------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| **ZonePolicy**     | Role applied to all Resources in a Zone           | `p, data-science-team, project:climate-analysis/*, read, *, *, allow`                                |
| **ResourcePolicy** | Role applied to a specific Resource               | `p, alice@kronicle, channel:climate-data, *, *, *, allow`                                            |
| **Delegated**      | Temporary Role delegated from one User to another | `p, service-account-1, channel:climate-data, write, 2023-01-01T00:00:00, 2023-01-02T00:00:00, allow` |
| **Anonymous**      | Default role for guests                           | `p, anonymous, public/*, read, *, *, allow`                                                          |

---

## Priority Rules

| Rule                                  | Notes                                               |
| ------------------------------------- | --------------------------------------------------- |
| **Restriction > Permission**          | Deny always overrides allow.                        |
| **User Policy > Group Policy**        | Direct User assignment overrides Group assignments. |
| **ResourcePolicy > ZonePolicy**       | Specific Resource rules override Zone-wide rules.   |
| **Delegated ≤ Delegator Permissions** | Cannot delegate more than your own permissions.     |

## Security safeguards

- **Bootstrap Protection:** Initial SuperUser created via CLI only.
- **Audit Logging:** Every RBAC decision, delegation, and default role application is logged.
- **Delegation Validation:** Delegator must hold the permission they are delegating, with optional time-bound expiration.

### Audit Logging

All authorization decisions are logged with the following context:

- Timestamp and Decision (Allow/Deny).
- Subject ID and Route Prefix used.
- Zone ID and Target Resource.

---

## Actions Table (Updated for Route Prefixes)

### System-Level Actions

| Resource / Action                   | Allowed Roles    | Route Prefix | Notes                                                                                     |
| ----------------------------------- | ---------------- | ------------ | ----------------------------------------------------------------------------------------- |
| Create Zone                         | Admin, SuperUser | `/setup/v1`  | When a zone is created, default roles for future children/resources must also be defined. |
| Assign Default Roles to Zone        | Admin, SuperUser | `/rbac/v1`   | Ensures that any resource created under the zone inherits these defaults.                 |
| Assign Users / Groups to Zone       | Admin, SuperUser | `/rbac/v1`   | Can assign Users/Groups to a Zone and optionally give them default roles.                 |
| Create User                         | Admin, SuperUser | `/rbac/v1`   | Limited to identity management.                                                           |
| Create Group                        | Admin, SuperUser | `/rbac/v1`   | Limited to identity management.                                                           |
| Assign Role to User or Group        | Admin, SuperUser | `/rbac/v1`   | Can attach roles for a Zone or Resource.                                                  |
| Manage RBAC (Policies, Delegations) | Admin, SuperUser | `/rbac/v1`   | Changes to Policies, Roles, and Delegations for Users/Groups.                             |

### Data-Level Actions

| Resource / Action        | Allowed Roles     | Route Prefix | Notes                                                                    |
| ------------------------ | ----------------- | ------------ | ------------------------------------------------------------------------ |
| Delegate Role/Permission | Writer, DataAdmin | `/rbac/v1`   | Can only delegate permissions they themselves hold.                      |
| Create Channel in Zone   | Writer, DataAdmin | `/data/v1`   | User automatically gets the default role for the Zone unless overridden. |
| Update Channel Metadata  | Writer, DataAdmin | `/setup/v1`  | Metadata changes are considered a resource admin action.                 |
| Delete Channel / Rows    | DataAdmin         | `/setup/v1`  | Full control over resources; restricted to highest privilege.            |
| Read Channel / Metadata  | Reader, Writer    | `/api/v1`    | Consumption lane; read-only access.                                      |

---

## Key Workflows

### 1. Zone creation (with default role propagation)

| Step | Action                                   | Example                                                                                                                                                       |
| ---- | ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1    | SuperUser or Admin creates Zone          | `zone_id = rbac_manager.create_zone(pool, "project:climate-analysis", superuser_id)`                                                                          |
| 2    | Assign default Roles for future children | `rbac_manager.set_zone_default_roles(pool, zone_id, {Reader, Writer})`                                                                                        |
| 3    | Assign Users / Groups to Zone            | `rbac_manager.assign_subject_to_zone(pool, zone_id, user_id, default_roles)`<br>`rbac_manager.assign_subject_to_zone(pool, zone_id, group_id, default_roles)` |
| 4    | Audit logging                            | `audit.log(action="create_zone", zone_id=zone_id, user=superuser_id)`                                                                                         |
| 5    | User creates Resource in Zone            | `channel_id = create_channel("climate-data", zone_id)`                                                                                                        |
| 6    | Default Roles applied automatically      | `rbac_manager.apply_zone_defaults(channel_id)`                                                                                                                |
| 7    | Audit logging                            | `audit.log(action="create_resource", resource_id=channel_id, creator=user_id, zone_id=zone_id)`                                                               |

---

### 2. Assigning ZoneAdmin

| Step | Action                                            | Example                                                                              |
| ---- | ------------------------------------------------- | ------------------------------------------------------------------------------------ |
| 1    | SuperUser creates Zone                            | `zone_id = rbac_manager.create_zone(pool, "project:climate-analysis", superuser_id)` |
| 2    | SuperUser assigns ZoneAdmin Role                  | `rbac_manager.assign_role_to_subject(pool, zone_id, user_id, Role.ZONE_ADMIN)`       |
| 3    | ZoneAdmin assigns Users / Groups to Zone          | `rbac_manager.assign_subject_to_zone(pool, zone_id, group_id, {Reader, Writer})`     |
| 4    | ZoneAdmin sets default roles for future Resources | `rbac_manager.set_zone_default_roles(pool, zone_id, {Reader, Writer})`               |

---

### 3. Delegating Access

| Step | Action                                   | Example                                                                                                                                        |
| ---- | ---------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| 1    | User delegates Role / Permission         | `await rbac_manager.delegate_access(pool, "alice@kronicle", "service-account-1", "channel:climate-data", "write", "2023-01-01", "2023-01-02")` |
| 2    | Enforcer checks current access           | `enforcer.enforce("service-account-1", "channel:climate-data", "write", "2023-01-01T12:00:00")`                                                |
| 3    | User revokes delegated Role / Permission | `enforcer.remove_policy("g", "service-account-1", "temporary-writer:alice@kronicle:channel:climate-data")`                                     |

---

## Notes on Default Roles

- **Preemptive rights**: Any Resource created inside a Zone inherits the default Roles automatically.
- **Override**: Admins or ZoneAdmins can override defaults per Resource after creation.
- **Propagation**: Can be per resource type: Metadata, Rows, or full Channel.
- **Audit**: Every role assignment via defaults is logged, even if automatic.
