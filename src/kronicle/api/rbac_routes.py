# kronicle/api/rbac_routes.py
from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query
from pydantic import EmailStr

from kronicle.auth.auth_middleware import require_auth
from kronicle.deps.rbac_deps import rbac_service
from kronicle.errors.error_types import BadRequestError, NotFoundError
from kronicle.schemas.rbac.input_group_schemas import InputGroup
from kronicle.schemas.rbac.input_user_schemas import InputUser
from kronicle.schemas.rbac.safe_group_schemas import OutputGroup
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
    return rbac.list_users()


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
# Group endpoints
# --------------------------------------------------------------------------------------------------


@rbac_router.post(
    "/groups",
    summary="Create a group",
    description="Creates a new RBAC group.",
    response_model=OutputGroup,
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
)
def remove_user_from_group(
    group_id: UUID,
    user_id: UUID,
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    rbac.remove_user_from_group(user_id=user_id, group_id=group_id)
    return {"detail": f"User '{user_id}' removed from group '{group_id}'"}
