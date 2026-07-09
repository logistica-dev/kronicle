# kronicle/api/rbac_routes.py
from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query
from pydantic import EmailStr

from kronicle.auth.auth_middleware import require_any_permission, require_auth, require_permission
from kronicle.deps.rbac_deps import rbac_service
from kronicle.errors.error_types import BadRequestError, NotFoundError
from kronicle.schemas.permissions.permission import PermStr
from kronicle.schemas.rbac.input_group_schemas import InputGroup
from kronicle.schemas.rbac.input_policy_schemas import (
    InputChannelAccessProfile,
    InputChannelPolicy,
    InputPolicyPatch,
    InputRowAccessProfile,
    InputRowPolicy,
    InputZoneAccessProfile,
    InputZonePolicy,
)
from kronicle.schemas.rbac.input_role_schemas import InputRole
from kronicle.schemas.rbac.input_user_schemas import InputUser, InputUserPatch
from kronicle.schemas.rbac.safe_group_schemas import OutputGroup
from kronicle.schemas.rbac.safe_policy_schemas import (
    OutputChannelAccessProfile,
    OutputChannelPolicy,
    OutputRowAccessProfile,
    OutputRowPolicy,
    OutputZoneAccessProfile,
    OutputZonePolicy,
)
from kronicle.schemas.rbac.safe_role_schemas import OutputRole
from kronicle.schemas.rbac.safe_user_schemas import OutputUser, ProcessedUser
from kronicle.services.rbac_service import RbacService

rbac_router = APIRouter(
    tags=["RBAC"],
    dependencies=[
        Depends(require_auth),
        Depends(require_any_permission(PermStr.RBAC_ACCESS_PROFILE, PermStr.RBAC_READ)),
    ],
)


@rbac_router.get(
    "/users",
    response_model=OutputUser | list[OutputUser] | None,
    dependencies=[Depends(require_permission(PermStr.USER_READ))],
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
    dependencies=[Depends(require_permission(PermStr.USER_READ))],
)
def get_user_by_id(
    user_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.fetch_user_by_id(user_id)


@rbac_router.post(
    "/users",
    response_model=OutputUser,
    dependencies=[Depends(require_permission(PermStr.USER_CREATE))],
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
    dependencies=[Depends(require_permission(PermStr.USER_UPDATE))],
)
def patch_user(
    user_in: InputUser,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    user_processed = ProcessedUser.from_input(user_in)
    return rbac.patch_user(user=user_processed)


@rbac_router.patch(
    "/users/{user_id}",
    response_model=OutputUser,
    dependencies=[Depends(require_permission(PermStr.USER_UPDATE))],
)
def patch_user_by_id(
    user_id: UUID,
    user_in: InputUserPatch | None = None,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    if not user_in:
        raise BadRequestError("No update data provided")
    return rbac.patch_user_by_id(
        user_id,
        name=user_in.name,
        full_name=user_in.full_name,
        orcid=user_in.orcid,
    )


@rbac_router.delete(
    "/users",
    response_model=OutputUser,
    dependencies=[Depends(require_permission(PermStr.USER_DELETE))],
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
    dependencies=[Depends(require_permission(PermStr.USER_DELETE))],
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


@rbac_router.put(
    "/users/{user_id}/roles/{role_id}",
    summary="Assign a role to a user",
    description="Grants a role directly to a user.",
    response_model=dict,
    dependencies=[Depends(require_permission(PermStr.ROLE_ASSIGN))],
)
def assign_role_to_user(
    user_id: UUID,
    role_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    rbac.assign_role_to_user(user_id=user_id, role_id=role_id)
    return {"detail": f"Role '{role_id}' assigned to user '{user_id}'"}


@rbac_router.delete(
    "/users/{user_id}/roles/{role_id}",
    summary="Remove a role from a user",
    description="Revokes a role directly from a user.",
    response_model=dict,
    dependencies=[Depends(require_permission(PermStr.ROLE_ASSIGN))],
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
    dependencies=[Depends(require_permission(PermStr.GROUP_CREATE))],
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
    dependencies=[Depends(require_permission(PermStr.GROUP_READ))],
)
def list_groups(
    name: str | None = Query(None, description="Optional name to filter by"),
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    if name:
        return rbac.get_group_by_name(name)
    return rbac.get_groups()


@rbac_router.get(
    "/groups/{group_id}",
    summary="Get a group by ID",
    description="Returns a single RBAC group.",
    response_model=OutputGroup,
    dependencies=[Depends(require_permission(PermStr.GROUP_READ))],
)
def get_group(
    group_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    group = rbac.get_group_by_id(group_id)
    if not group:
        raise NotFoundError(f"Group '{group_id}' not found")
    return group


@rbac_router.patch(
    "/groups/{group_id}",
    summary="Update a group",
    description="Partially update a group's name or details.",
    response_model=OutputGroup,
    dependencies=[Depends(require_permission(PermStr.GROUP_UPDATE))],
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
    dependencies=[Depends(require_permission(PermStr.GROUP_DELETE))],
)
def delete_group(
    group_id: UUID,
    force: bool | None = Query(False, description="Force deletion even if users are assigned"),
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    group = rbac.delete_group(group_id, force=force or False)
    if not group:
        raise NotFoundError(f"Group '{group_id}' not found")
    return group


@rbac_router.post(
    "/groups/{group_id}/users",
    summary="Add user to group",
    description="Adds a user as member of a group.",
    response_model=dict,
    dependencies=[Depends(require_permission(PermStr.GROUP_ASSIGN))],
)
def add_user_to_group(
    group_id: UUID,
    user_id: UUID = Query(..., description="UUID of the user to add"),  # noqa: B008
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    rbac.add_user_to_group(user_id=user_id, group_id=group_id)
    return {"detail": f"User '{user_id}' added to group '{group_id}'"}


@rbac_router.get(
    "/groups/{group_id}/users",
    summary="Get users from group",
    description="Returns all users assigned to the identified group.",
    response_model=list[OutputUser],
    dependencies=[Depends(require_permission(PermStr.GROUP_READ))],
)
def get_users_from_group(
    group_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    if not group_id:
        return []
    return rbac.get_users_from_group(group_id=group_id)


@rbac_router.delete(
    "/groups/{group_id}/users/{user_id}",
    summary="Remove user from group",
    description="Removes a user from a group.",
    response_model=dict,
    dependencies=[Depends(require_permission(PermStr.GROUP_ASSIGN))],
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


@rbac_router.put(
    "/groups/{group_id}/roles/{role_id}",
    summary="Assign a role to a group",
    description="Grants a role to all members of a group.",
    response_model=dict,
    dependencies=[Depends(require_permission(PermStr.ROLE_ASSIGN))],
)
def assign_role_to_group(
    group_id: UUID,
    role_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    rbac.assign_role_to_group(group_id=group_id, role_id=role_id)
    return {"detail": f"Role '{role_id}' assigned to group '{group_id}'"}


@rbac_router.delete(
    "/groups/{group_id}/roles/{role_id}",
    summary="Remove a role from a group",
    description="Revokes a role from all members of a group.",
    response_model=dict,
    dependencies=[Depends(require_permission(PermStr.ROLE_ASSIGN))],
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
    dependencies=[Depends(require_permission(PermStr.ROLE_CREATE))],
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
    response_model=OutputRole | list[OutputRole] | None,
    dependencies=[Depends(require_permission(PermStr.ROLE_READ))],
)
def list_roles(
    name: str | None = Query(None, description="Optional name to filter by"),
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    if name:
        return rbac.get_role_by_name(name)
    return rbac.get_roles()


@rbac_router.get(
    "/roles/{role_id}",
    summary="Get a role by ID",
    description="Returns a single RBAC role.",
    response_model=OutputRole,
    dependencies=[Depends(require_permission(PermStr.ROLE_READ))],
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
    dependencies=[Depends(require_permission(PermStr.ROLE_UPDATE))],
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
    dependencies=[Depends(require_permission(PermStr.ROLE_DELETE))],
)
def delete_role(
    role_id: UUID,
    force: bool | None = Query(False, description="Force deletion even if users/groups are assigned"),
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    role = rbac.delete_role(role_id, force=force or False)
    if not role:
        raise NotFoundError(f"Role '{role_id}' not found")
    return role


# --------------------------------------------------------------------------------------------------
# Relationship check endpoints
# --------------------------------------------------------------------------------------------------


@rbac_router.get(
    "/users/{user_id}/roles/{role_id}",
    summary="Check if a role is assigned to a user",
    description="Returns whether a role is assigned to a user (directly or via group membership).",
    dependencies=[Depends(require_permission(PermStr.USER_READ))],
)
def check_user_role(
    user_id: UUID,
    role_id: UUID,
    indirect: bool = Query(False, description="Include indirect assignments via group membership"),
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.check_user_has_role(user_id=user_id, role_id=role_id, indirect=indirect)


@rbac_router.get(
    "/groups/{group_id}/roles/{role_id}",
    summary="Check if a role is assigned to a group",
    description="Returns whether a role is assigned to a group (directly or via parent groups).",
    dependencies=[Depends(require_permission(PermStr.GROUP_READ))],
)
def check_group_role(
    group_id: UUID,
    role_id: UUID,
    indirect: bool = Query(False, description="Include indirect assignments via parent groups"),
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.check_group_has_role(group_id=group_id, role_id=role_id, indirect=indirect)


@rbac_router.get(
    "/roles/{role_id}/subjects",
    summary="List subjects assigned to a role",
    description="Returns users and groups assigned to a role (direct or indirect).",
    dependencies=[Depends(require_permission(PermStr.ROLE_READ))],
)
def list_role_subjects(
    role_id: UUID,
    indirect: bool = Query(False, description="Include indirect subjects via group hierarchy"),
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.list_role_subjects(role_id=role_id, indirect=indirect)


@rbac_router.get(
    "/users/{user_id}/groups/{group_id}",
    summary="Check if a user belongs to a group",
    description="Returns whether a user is a member of a group (directly or via sub-groups).",
    dependencies=[Depends(require_permission(PermStr.GROUP_READ))],
)
def check_user_group(
    user_id: UUID,
    group_id: UUID,
    indirect: bool = Query(False, description="Include indirect membership via sub-groups"),
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.check_user_in_group(user_id=user_id, group_id=group_id, indirect=indirect)


# --------------------------------------------------------------------------------------------------
# Access Profiles – reusable scoped roles
# --------------------------------------------------------------------------------------------------


@rbac_router.get(
    "/access-profiles",
    summary="List all zone access profiles",
    response_model=list[OutputZoneAccessProfile],
    dependencies=[Depends(require_permission(PermStr.POLICY_READ))],
)
def list_access_profiles(
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.list_access_profiles()


@rbac_router.post(
    "/access-profiles/zones",
    summary="Create a zone access profile",
    description="Creates a reusable scoped role (role + zone pair).",
    response_model=OutputZoneAccessProfile,
    dependencies=[Depends(require_permission(PermStr.POLICY_CREATE))],
)
def create_zone_access_profile(
    profile_in: InputZoneAccessProfile,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):

    return rbac.create_zone_access_profile(profile_in=profile_in)


@rbac_router.get(
    "/access-profiles/zones",
    summary="List all zone access profiles",
    response_model=list[OutputZoneAccessProfile],
    dependencies=[Depends(require_permission(PermStr.POLICY_READ))],
)
def list_zone_access_profiles(
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.list_zone_access_profiles()


@rbac_router.get(
    "/access-profiles/zones/{profile_id}",
    summary="Get a zone access profile",
    response_model=OutputZoneAccessProfile,
    dependencies=[Depends(require_permission(PermStr.POLICY_READ))],
)
def get_zone_access_profile(
    profile_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    profile = rbac.get_zone_access_profile(profile_id)
    if not profile:
        raise NotFoundError(f"ZoneAccessProfile '{profile_id}' not found")
    return profile


@rbac_router.delete(
    "/access-profiles/zones/{profile_id}",
    summary="Delete a zone access profile",
    dependencies=[Depends(require_permission(PermStr.POLICY_DELETE))],
)
def delete_zone_access_profile(
    profile_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    rbac.delete_zone_access_profile(profile_id)
    return {"detail": f"ZoneAccessProfile '{profile_id}' deleted"}


@rbac_router.post(
    "/access-profiles/channels",
    summary="Create a channel access profile",
    description="Creates a reusable scoped role (role + channel pair).",
    response_model=OutputChannelAccessProfile,
    dependencies=[Depends(require_permission(PermStr.POLICY_CREATE))],
)
def create_channel_access_profile(
    profile_in: InputChannelAccessProfile,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.create_channel_access_profile(profile_in=profile_in)


@rbac_router.get(
    "/access-profiles/channels",
    summary="List all channel access profiles",
    response_model=list[OutputChannelAccessProfile],
    dependencies=[Depends(require_permission(PermStr.POLICY_READ))],
)
def list_channel_access_profiles(
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.list_channel_access_profiles()


@rbac_router.get(
    "/access-profiles/channels/{profile_id}",
    summary="Get a channel access profile",
    response_model=OutputChannelAccessProfile,
    dependencies=[Depends(require_permission(PermStr.POLICY_READ))],
)
def get_channel_access_profile(
    profile_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    profile = rbac.get_channel_access_profile(profile_id)
    if not profile:
        raise NotFoundError(f"ChannelAccessProfile '{profile_id}' not found")
    return profile


@rbac_router.delete(
    "/access-profiles/channels/{profile_id}",
    summary="Delete a channel access profile",
    dependencies=[Depends(require_permission(PermStr.POLICY_DELETE))],
)
def delete_channel_access_profile(
    profile_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    rbac.delete_channel_access_profile(profile_id)
    return {"detail": f"ChannelAccessProfile '{profile_id}' deleted"}


# --------------------------------------------------------------------------------------------------
# Zone Policy endpoints
# --------------------------------------------------------------------------------------------------


@rbac_router.post(
    "/policies/zones",
    summary="Assign a role to a subject for a zone",
    description="Creates a policy that grants a role to a user or group for a specific zone.",
    response_model=OutputZonePolicy,
    dependencies=[Depends(require_permission(PermStr.POLICY_CREATE))],
)
def create_zone_policy(
    policy_in: InputZonePolicy,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.create_zone_policy(
        subject=policy_in.subject,
        access_profile=policy_in.access_profile,
        name=policy_in.name,
        details=policy_in.details,
    )


@rbac_router.get(
    "/policies/zones/{zone_id}",
    summary="List policies for a zone",
    description="Returns all policies assigned for a specific zone.",
    response_model=list[dict],
    dependencies=[Depends(require_permission(PermStr.POLICY_READ))],
)
def list_policies_for_zone(
    zone_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.list_policies_for_zone(zone_id)


@rbac_router.delete(
    "/policies/zones/{policy_id}",
    summary="Delete a zone policy",
    description="Removes a zone policy by its ID.",
    dependencies=[Depends(require_permission(PermStr.POLICY_DELETE))],
)
def delete_zone_policy(
    policy_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    rbac.delete_zone_policy(policy_id)
    return {"detail": f"ZonePolicy '{policy_id}' deleted"}


@rbac_router.patch(
    "/policies/zones/{policy_id}",
    summary="Patch a zone policy",
    description="Partially update a zone policy's name or details.",
    response_model=OutputZonePolicy,
    dependencies=[Depends(require_permission(PermStr.POLICY_UPDATE))],
)
def patch_zone_policy(
    policy_id: UUID,
    patch_in: InputPolicyPatch | None = None,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    name = patch_in.name if patch_in else None
    details = patch_in.details if patch_in else None
    return rbac.patch_zone_policy(policy_id, name=name, details=details)


# --------------------------------------------------------------------------------------------------
# Channel Policy endpoints
# --------------------------------------------------------------------------------------------------


@rbac_router.post(
    "/policies/channels",
    summary="Assign a role to a subject for a channel",
    description="Creates a policy that grants a role to a user or group for a specific channel.",
    response_model=OutputChannelPolicy,
    dependencies=[Depends(require_permission(PermStr.POLICY_CREATE))],
)
def create_channel_policy(
    policy_in: InputChannelPolicy,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.create_channel_policy(
        subject=policy_in.subject,
        access_profile=policy_in.access_profile,
    )


@rbac_router.get(
    "/policies/channels/{channel_id}",
    summary="List policies for a channel",
    description="Returns all policies assigned for a specific channel.",
    response_model=list[dict],
    dependencies=[Depends(require_permission(PermStr.POLICY_READ))],
)
def list_policies_for_channel(
    channel_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.list_policies_for_channel(channel_id)


@rbac_router.delete(
    "/policies/channels/{policy_id}",
    summary="Delete a channel policy",
    description="Removes a channel policy by its ID.",
    dependencies=[Depends(require_permission(PermStr.POLICY_DELETE))],
)
def delete_channel_policy(
    policy_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    rbac.delete_channel_policy(policy_id)
    return {"detail": f"ChannelPolicy '{policy_id}' deleted"}


@rbac_router.patch(
    "/policies/channels/{policy_id}",
    summary="Patch a channel policy",
    description="Partially update a channel policy's name or details.",
    response_model=OutputChannelPolicy,
    dependencies=[Depends(require_permission(PermStr.POLICY_UPDATE))],
)
def patch_channel_policy(
    policy_id: UUID,
    patch_in: InputPolicyPatch | None = None,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    name = patch_in.name if patch_in else None
    details = patch_in.details if patch_in else None
    return rbac.patch_channel_policy(policy_id, name=name, details=details)


# --------------------------------------------------------------------------------------------------
# Row Access Profiles
# --------------------------------------------------------------------------------------------------


@rbac_router.post(
    "/access-profiles/rows",
    summary="Create a row access profile",
    description="Creates a reusable scoped role (role + row pair).",
    response_model=OutputRowAccessProfile,
    dependencies=[Depends(require_permission(PermStr.POLICY_CREATE))],
)
def create_row_access_profile(
    profile_in: InputRowAccessProfile,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.create_row_access_profile(profile_in=profile_in)


@rbac_router.get(
    "/access-profiles/rows",
    summary="List all row access profiles",
    response_model=list[OutputRowAccessProfile],
    dependencies=[Depends(require_permission(PermStr.POLICY_READ))],
)
def list_row_access_profiles(
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.list_row_access_profiles()


@rbac_router.get(
    "/access-profiles/rows/{profile_id}",
    summary="Get a row access profile",
    response_model=OutputRowAccessProfile,
    dependencies=[Depends(require_permission(PermStr.POLICY_READ))],
)
def get_row_access_profile(
    profile_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    profile = rbac.get_row_access_profile(profile_id)
    if not profile:
        raise NotFoundError(f"RowAccessProfile '{profile_id}' not found")
    return profile


@rbac_router.delete(
    "/access-profiles/rows/{profile_id}",
    summary="Delete a row access profile",
    dependencies=[Depends(require_permission(PermStr.POLICY_DELETE))],
)
def delete_row_access_profile(
    profile_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    rbac.delete_row_access_profile(profile_id)
    return {"detail": f"RowAccessProfile '{profile_id}' deleted"}


# --------------------------------------------------------------------------------------------------
# List all policies (by resource type + global)
# --------------------------------------------------------------------------------------------------


@rbac_router.get(
    "/policies",
    summary="List all policies across all resource types",
    description="Returns a dict with 'zone', 'channel', and 'row' keys containing all policies.",
    dependencies=[Depends(require_permission(PermStr.POLICY_READ))],
)
def list_policies(
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.list_policies()


@rbac_router.get(
    "/policies/zones",
    summary="List all zone policies",
    description="Returns all zone policies regardless of zone.",
    response_model=list[OutputZonePolicy],
    dependencies=[Depends(require_permission(PermStr.POLICY_READ))],
)
def list_zone_policies(
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.list_zone_policies()


@rbac_router.get(
    "/policies/channels",
    summary="List all channel policies",
    description="Returns all channel policies regardless of channel.",
    response_model=list[OutputChannelPolicy],
    dependencies=[Depends(require_permission(PermStr.POLICY_READ))],
)
def list_channel_policies(
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.list_channel_policies()


@rbac_router.get(
    "/policies/rows",
    summary="List all row policies",
    description="Returns all row policies regardless of row.",
    response_model=list[OutputRowPolicy],
    dependencies=[Depends(require_permission(PermStr.POLICY_READ))],
)
def list_row_policies(
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.list_row_policies()


# --------------------------------------------------------------------------------------------------
# Row Policy endpoints
# --------------------------------------------------------------------------------------------------


@rbac_router.post(
    "/policies/rows",
    summary="Assign a role to a subject for a row",
    description="Creates a policy that grants a role to a user or group for a specific row.",
    response_model=OutputRowPolicy,
    dependencies=[Depends(require_permission(PermStr.POLICY_CREATE))],
)
def create_row_policy(
    policy_in: InputRowPolicy,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.create_row_policy(
        subject=policy_in.subject,
        access_profile=policy_in.access_profile,
        name=policy_in.name,
        details=policy_in.details,
    )


@rbac_router.get(
    "/policies/rows/{row_id}",
    summary="List policies for a row",
    description="Returns all policies assigned for a specific row.",
    response_model=list[dict],
    dependencies=[Depends(require_permission(PermStr.POLICY_READ))],
)
def list_policies_for_row(
    row_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    return rbac.list_policies_for_row(row_id)


@rbac_router.delete(
    "/policies/rows/{policy_id}",
    summary="Delete a row policy",
    description="Removes a row policy by its ID.",
    dependencies=[Depends(require_permission(PermStr.POLICY_DELETE))],
)
def delete_row_policy(
    policy_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    rbac.delete_row_policy(policy_id)
    return {"detail": f"RowPolicy '{policy_id}' deleted"}


@rbac_router.patch(
    "/policies/rows/{policy_id}",
    summary="Patch a row policy",
    description="Partially update a row policy's name or details.",
    response_model=OutputRowPolicy,
    dependencies=[Depends(require_permission(PermStr.POLICY_UPDATE))],
)
def patch_row_policy(
    policy_id: UUID,
    patch_in: InputPolicyPatch | None = None,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    name = patch_in.name if patch_in else None
    details = patch_in.details if patch_in else None
    return rbac.patch_row_policy(policy_id, name=name, details=details)
