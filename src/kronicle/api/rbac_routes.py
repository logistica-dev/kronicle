# kronicle/api/rbac_routes.py
from fastapi import APIRouter, Depends, Query
from pydantic import EmailStr

from kronicle.auth.auth_middleware import require_auth
from kronicle.deps.rbac_deps import rbac_service
from kronicle.errors.error_types import BadRequestError
from kronicle.schemas.rbac.input_user_schemas import InputUser
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
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
    user_processed = ProcessedUser.from_input(user_in)
    return rbac.delete_user(user=user_processed)
