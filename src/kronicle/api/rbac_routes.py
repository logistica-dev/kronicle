# kronicle/api/rbac_routes.py
from fastapi import APIRouter, Depends

from kronicle.auth.auth_middleware import require_auth
from kronicle.deps.rbac_deps import rbac_service
from kronicle.schemas.rbac.user_schemas import InputUser, OutputUser, ProcessedUser
from kronicle.services.rbac_service import RbacService

rbac_router = APIRouter(tags=["RBAC"], dependencies=[Depends(require_auth)])


@rbac_router.get(
    "/users",
    response_model=list[OutputUser],
)
def list_users(
    rbac: RbacService = Depends(rbac_service),  # noqa: B008
):
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
