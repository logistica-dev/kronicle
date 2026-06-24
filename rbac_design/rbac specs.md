# RBAC specs

This documents tries to express the needs in Kronicle for an RBAC system that enables/prenvents some behavior for users and user groups.

Let's say we have

- UserA, UserB and UserC in Group1
- UserD, UserE and UserF in Group2
- ZoneA with Channel1, Channel2, Channel3
- ZoneB with Channel4, Channel5, Channel6

Bellow are behaviors that the rbac system should enable/prevent.

1. UserA is ZoneA admin.
   - They cannot access ZoneB.
   - They can add, update, delete, add rows to a new channel in ZoneA.
   - They can grant other users the right to add, update, delete, add rows to a new channel in ZoneA

2. UserB has been granted "add channel" access to ZoneA.
   - They cannot access ZoneB.
   - They can read other user's channels in ZoneA.
   - They can add a new Channel. When they do so, they are granted preemptive rights to this channel:
     - they can add rows to this Channel, and grant access to another user
