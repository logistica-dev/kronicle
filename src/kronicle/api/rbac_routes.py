# kronicle/api/rbac_routes.py
from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query
from pydantic import EmailStr

from kronicle.auth.auth_middleware import require_auth, require_permission
from kronicle.schemas.permissions.permission import Permission, PermissionAction, PermissionTarget
from kronicle.deps.rbac_deps import rbac_service
from kronicle.errors.error_types import BadRequestError, NotFoundError
from kronicle.schemas.rbac.input_group_schemas import InputGroup
from kronicle.schemas.rbac.input_policy_schemas import InputChannelPolicy, InputZonePolicy
from kronicle.schemas.rbac.input_role_schemas import InputRole
from kronicle.schemas.rbac.input_user_schemas import InputUser
from kronicle.schemas.rbac.safe_group_schemas import OutputGroup
from kronicle.schemas.rbac.safe_role_schemas import OutputRole
from kronicle.schemas.rbac.safe_user_schemas import OutputUser, ProcessedUser
from kronicle.services.rbac_service import RbacService

rbac_router = APIRouter(tags=["RBAC"], dependencies=[Depends(require_auth)])


@rbac_router.get(
    "/users",
    response_model=OutputUser | list[OutputUser] | None,
)
def list_users(
    email: EmailStr | None = Query(None, description="Optional email to filter by"),  # noqa: B008
    name: str | None = Query(None, description="Optional name to filter by"),
    orcid: str | None = Query(None, description="Optional ORCID to filter by"),
    include_inactive: bool | None = Query(False, description="Optional flag, list includes inactive users if True"),
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    if email:
        return rbac.fetch_user_by_email(email)
    if name:
        return rbac.fetch_user_by_name(name)
    if orcid:
        return rbac.fetch_user_by_external_id(orcid)
    for query in [email, name, orcid]:
        if query is not None:
            raise BadRequestError(f"Query {query} cannot be empty")
    return rbac.list_users(include_inactive=True if include_inactive is True else False)


@rbac_router.get(
    "/users/{user_id}",
    response_model=OutputUser | list[OutputUser] | None,
)
def get_user_by_id(
    user_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.fetch_user_by_id(user_id)


@rbac_router.post(
    "/users",
    response_model=OutputUser,
    dependencies=[Depends(require_permission(Permission(PermissionTarget.USER, PermissionAction.CREATE)))],
)
def create_user(
    user_in: InputUser,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    user_processed = ProcessedUser.from_input(user_in)
    return rbac.create_user(user=user_processed)


@rbac_router.patch(
    "/users",
    response_model=OutputUser,
    dependencies=[Depends(require_permission(Permission(PermissionTarget.USER, PermissionAction.UPDATE)))],
)
def patch_user(
    user_in: InputUser,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    user_processed = ProcessedUser.from_input(user_in)
    return rbac.patch_user(user=user_processed)


@rbac_router.delete(
    "/users",
    response_model=OutputUser,
    dependencies=[Depends(require_permission(Permission(PermissionTarget.USER, PermissionAction.DELETE)))],
)
def delete_user(
    user_in: InputUser,
    remove: bool | None = Query(False, description="Remove user from DB if True"),
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    user_processed = ProcessedUser.from_input(user_in)
    if remove:
        return rbac.remove_user(user=user_processed)
    return rbac.deactivate_user(user=user_processed)


@rbac_router.delete(
    "/users/{user_id}",
    response_model=OutputUser,
    dependencies=[Depends(require_permission(Permission(PermissionTarget.USER, PermissionAction.DELETE)))],
)
def delete_user_by_id(
    user_id: UUID,
    remove: bool | None = Query(False, description="Remove user from DB if True"),
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    if remove:
        return rbac.remove_user_by_id(id=user_id)
    return rbac.deactivate_user_by_id(id=user_id)


# --------------------------------------------------------------------------------------------------
# User ↔ Role endpoints
# --------------------------------------------------------------------------------------------------


@rbac_router.post(
    "/users/{user_id}/roles",
    summary="Assign a role to a user",
    description="Grants a role directly to a user.",
    response_model=dict,
    dependencies=[Depends(require_permission(Permission(PermissionTarget.ROLE, PermissionAction.ASSIGN)))],
)
def assign_role_to_user(
    user_id: UUID,
    role_id: UUID = Query(..., description="UUID of the role to assign"),  # noqa: B008
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    rbac.assign_role_to_user(user_id=user_id, role_id=role_id)
    return {"detail": f"Role '{role_id}' assigned to user '{user_id}'"}


@rbac_router.delete(
    "/users/{user_id}/roles/{role_id}",
    summary="Remove a role from a user",
    description="Revokes a role directly from a user.",
    response_model=dict,
    dependencies=[Depends(require_permission(Permission(PermissionTarget.ROLE, PermissionAction.ASSIGN)))],
)
def remove_role_from_user(
    user_id: UUID,
    role_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    rbac.remove_role_from_user(user_id=user_id, role_id=role_id)
    return {"detail": f"Role '{role_id}' removed from user '{user_id}'"}


# --------------------------------------------------------------------------------------------------
# Group endpoints
# --------------------------------------------------------------------------------------------------


@rbac_router.post(
    "/groups",
    summary="Create a group",
    description="Creates a new RBAC group.",
    response_model=OutputGroup,
    dependencies=[Depends(require_permission(Permission(PermissionTarget.GROUP, PermissionAction.CREATE)))],
)
def create_group(
    group_in: InputGroup,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.create_group(name=group_in.name, details=group_in.details)


@rbac_router.get(
    "/groups",
    summary="List all groups",
    description="Returns all RBAC groups.",
    response_model=list[OutputGroup],
)
def list_groups(
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.get_groups()


@rbac_router.get(
    "/groups/{group_id}",
    summary="Get a group by ID",
    description="Returns a single RBAC group.",
    response_model=OutputGroup,
)
def get_group(
    group_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    group = rbac.get_group(group_id)
    if not group:
        raise NotFoundError(f"Group '{group_id}' not found")
    return group


@rbac_router.patch(
    "/groups/{group_id}",
    summary="Update a group",
    description="Partially update a group's name or details.",
    response_model=OutputGroup,
    dependencies=[Depends(require_permission(Permission(PermissionTarget.GROUP, PermissionAction.UPDATE)))],
)
def patch_group(
    group_id: UUID,
    group_in: InputGroup | None = None,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    name = group_in.name if group_in else None
    details = group_in.details if group_in else None
    return rbac.patch_group(group_id, name=name, details=details)


@rbac_router.delete(
    "/groups/{group_id}",
    summary="Delete a group",
    description="Deletes an RBAC group.",
    response_model=OutputGroup,
    dependencies=[Depends(require_permission(Permission(PermissionTarget.GROUP, PermissionAction.DELETE)))],
)
def delete_group(
    group_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    group = rbac.delete_group(group_id)
    if not group:
        raise NotFoundError(f"Group '{group_id}' not found")
    return group


@rbac_router.post(
    "/groups/{group_id}/users",
    summary="Add user to group",
    description="Adds a user as member of a group.",
    response_model=dict,
    dependencies=[Depends(require_permission(Permission(PermissionTarget.GROUP, PermissionAction.ASSIGN)))],
)
def add_user_to_group(
    group_id: UUID,
    user_id: UUID = Query(..., description="UUID of the user to add"),  # noqa: B008
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    rbac.add_user_to_group(user_id=user_id, group_id=group_id)
    return {"detail": f"User '{user_id}' added to group '{group_id}'"}


@rbac_router.delete(
    "/groups/{group_id}/users/{user_id}",
    summary="Remove user from group",
    description="Removes a user from a group.",
    response_model=dict,
    dependencies=[Depends(require_permission(Permission(PermissionTarget.GROUP, PermissionAction.ASSIGN)))],
)
def remove_user_from_group(
    group_id: UUID,
    user_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    rbac.remove_user_from_group(user_id=user_id, group_id=group_id)
    return {"detail": f"User '{user_id}' removed from group '{group_id}'"}


# --------------------------------------------------------------------------------------------------
# Group ↔ Role endpoints
# --------------------------------------------------------------------------------------------------


@rbac_router.post(
    "/groups/{group_id}/roles",
    summary="Assign a role to a group",
    description="Grants a role to all members of a group.",
    response_model=dict,
    dependencies=[Depends(require_permission(Permission(PermissionTarget.ROLE, PermissionAction.ASSIGN)))],
)
def assign_role_to_group(
    group_id: UUID,
    role_id: UUID = Query(..., description="UUID of the role to assign"),  # noqa: B008
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    rbac.assign_role_to_group(group_id=group_id, role_id=role_id)
    return {"detail": f"Role '{role_id}' assigned to group '{group_id}'"}


@rbac_router.delete(
    "/groups/{group_id}/roles/{role_id}",
    summary="Remove a role from a group",
    description="Revokes a role from all members of a group.",
    response_model=dict,
    dependencies=[Depends(require_permission(Permission(PermissionTarget.ROLE, PermissionAction.ASSIGN)))],
)
def remove_role_from_group(
    group_id: UUID,
    role_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    rbac.remove_role_from_group(group_id=group_id, role_id=role_id)
    return {"detail": f"Role '{role_id}' removed from group '{group_id}'"}


# --------------------------------------------------------------------------------------------------
# Role endpoints
# --------------------------------------------------------------------------------------------------


@rbac_router.post(
    "/roles",
    summary="Create a role",
    description="Creates a new RBAC role with permissions and restrictions.",
    response_model=OutputRole,
    dependencies=[Depends(require_permission(Permission(PermissionTarget.ROLE, PermissionAction.CREATE)))],
)
def create_role(
    role_in: InputRole,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.create_role(
        name=role_in.name,
        description=role_in.description,
        permissions=role_in.permissions,
        restrictions=role_in.restrictions,
        details=role_in.details,
    )


@rbac_router.get(
    "/roles",
    summary="List all roles",
    description="Returns all RBAC roles.",
    response_model=list[OutputRole],
)
def list_roles(
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.get_roles()


@rbac_router.get(
    "/roles/{role_id}",
    summary="Get a role by ID",
    description="Returns a single RBAC role.",
    response_model=OutputRole,
)
def get_role(
    role_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    role = rbac.get_role(role_id)
    if not role:
        raise NotFoundError(f"Role '{role_id}' not found")
    return role


@rbac_router.patch(
    "/roles/{role_id}",
    summary="Update a role",
    description="Partially update a role's name, description, permissions, or restrictions.",
    response_model=OutputRole,
    dependencies=[Depends(require_permission(Permission(PermissionTarget.ROLE, PermissionAction.UPDATE)))],
)
def patch_role(
    role_id: UUID,
    role_in: InputRole | None = None,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    name = role_in.name if role_in else None
    description = role_in.description if role_in else None
    permissions = role_in.permissions if role_in else None
    restrictions = role_in.restrictions if role_in else None
    details = role_in.details if role_in else None
    return rbac.patch_role(
        role_id,
        name=name,
        description=description,
        permissions=permissions,
        restrictions=restrictions,
        details=details,
    )


@rbac_router.delete(
    "/roles/{role_id}",
    summary="Delete a role",
    description="Deletes an RBAC role.",
    response_model=OutputRole,
    dependencies=[Depends(require_permission(Permission(PermissionTarget.ROLE, PermissionAction.DELETE)))],
)
def delete_role(
    role_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    role = rbac.delete_role(role_id)
    if not role:
        raise NotFoundError(f"Role '{role_id}' not found")
    return role


# --------------------------------------------------------------------------------------------------
# Zone Policy endpoints
# --------------------------------------------------------------------------------------------------


@rbac_router.post(
    "/policies/zones",
    summary="Assign a role to a subject for a zone",
    description="Creates a policy that grants a role to a user or group for a specific zone.",
    response_model=dict,
    dependencies=[Depends(require_permission(Permission(PermissionTarget.POLICY, PermissionAction.CREATE)))],
)
def create_zone_policy(
    policy_in: InputZonePolicy,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.create_zone_policy(
        subject_id=policy_in.subject_id,
        role_id=policy_in.role_id,
        zone_id=policy_in.zone_id,
    )


@rbac_router.get(
    "/policies/zones/{zone_id}",
    summary="List policies for a zone",
    description="Returns all policies assigned for a specific zone.",
    response_model=list[dict],
)
def list_zone_policies(
    zone_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.list_zone_policies(zone_id)


@rbac_router.delete(
    "/policies/zones/{policy_id}",
    summary="Delete a zone policy",
    description="Removes a zone policy by its ID.",
    dependencies=[Depends(require_permission(Permission(PermissionTarget.POLICY, PermissionAction.DELETE)))],
)
def delete_zone_policy(
    policy_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    rbac.delete_zone_policy(policy_id)
    return {"detail": f"ZonePolicy '{policy_id}' deleted"}


# --------------------------------------------------------------------------------------------------
# Channel Policy endpoints
# --------------------------------------------------------------------------------------------------


@rbac_router.post(
    "/policies/channels",
    summary="Assign a role to a subject for a channel",
    description="Creates a policy that grants a role to a user or group for a specific channel.",
    response_model=dict,
    dependencies=[Depends(require_permission(Permission(PermissionTarget.POLICY, PermissionAction.CREATE)))],
)
def create_channel_policy(
    policy_in: InputChannelPolicy,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.create_channel_policy(
        subject_id=policy_in.subject_id,
        role_id=policy_in.role_id,
        channel_id=policy_in.channel_id,
    )


@rbac_router.get(
    "/policies/channels/{channel_id}",
    summary="List policies for a channel",
    description="Returns all policies assigned for a specific channel.",
    response_model=list[dict],
)
def list_channel_policies(
    channel_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.list_channel_policies(channel_id)


@rbac_router.delete(
    "/policies/channels/{policy_id}",
    summary="Delete a channel policy",
    description="Removes a channel policy by its ID.",
    dependencies=[Depends(require_permission(Permission(PermissionTarget.POLICY, PermissionAction.DELETE)))],
)
def delete_channel_policy(
    policy_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    rbac.delete_channel_policy(policy_id)
    return {"detail": f"ChannelPolicy '{policy_id}' deleted"}
