# Expressing RBAC needs in terms of access to resources

This documents tries to express the needs in Kronicle for an RBAC system that enables/prenvents some behavior for users and user groups.

Let's say we have

- UserA, UserB and UserC in Group1
- UserD, UserE and UserF in Group2
- ZoneA with Channel1, Channel2, Channel3
- ZoneB with Channel4, Channel5, Channel6
- ZoneA and ZoneB are unrelated
- ZoneC is a child of ZoneA

Bellow are behaviors that the rbac system should enable/prevent.

1. Say UserA has been `delegated` _admin role_ on ZoneA (and ZoneA only) by the superuser.
   - UserA cannot access ZoneB (not even `read`).
   - UserA can `add`, `read`, `update`, `remove` any channel in ZoneA or ZoneC.
   - UserA can `add`, `read`, `update`, `remove` rows to a any channel in ZoneA and ZoneC.
   - UserA can `delegate` other users and groups the privilege to `add`, `read`, `update`, `remove` a channel in ZoneA or ZoneC
   - UserA can `delegate` other users and groups the privilege to `add`, `read`, `update`, `remove` rows to a channel in ZoneA or ZoneC.
   - Additionally, UserA may grant other users/groups the privilege to `delegate` their own rights (including `delegate`) to other identified users.

2. Say UserB has been delegated `add` (channels) access to ZoneA (but not `read` access to ZoneA).
   - UserB cannot access ZoneB (not even `read`) nor ZoneC.
   - UserB cannot `read` other channels in ZoneA.
   - UserB can `add` (create) a new Channel7 in ZoneA. When they do so, they are delegated preemptive rights to this channel:
     - UserB can `read`, `add`, `update`, `remove` metadata and rows for Channel7
     - UserB can `delegate` **up to their own rights** on Channel7 to any identified user or group, e.g. UserC. In such case, UserC will see both Channel7's metadata and every Channel7's row.
     - UserB can `delegate` `read` access to only some rows of Channel7 to an identified user or group, e.g. group2. In such case, UserD, UserE and UserF can `read` Channel7's metadata and only a subset of Channel7's rows.
     - UserB can `delegate` anonymous users `read` access to the full Channel7 or a subset of rows of Channel7.
     - UserB can `delegate` `add` (rows) access to any identified user

3. Anonymous users can at most be delegated `read` rights, never `add`/`update`/`remove`/`delegate`...

4. The permission `delegate`, means a user can grant some of their own rights on a resource to another identified user.
