# kronicle/api/auth_routes.py
from fastapi import APIRouter, Body, Depends
from fastapi.security import OAuth2PasswordBearer

from kronicle.auth.auth_service import AuthService
from kronicle.deps.rbac_deps import auth_service
from kronicle.schemas.rbac.input_user_schemas import InputUserChangePwd, InputUserLogin

auth_router = APIRouter(tags=["Auth"])


@auth_router.post("/login")
def login(
    creds: InputUserLogin = Body(...),  # noqa: B008
    auth: AuthService = Depends(auth_service),  # noqa: B008
):
    token = auth.login(creds)
    return {"access_token": token, "token_type": "bearer"}


OAuth2PasswordBearer(tokenUrl="/auth/v1/login")


@auth_router.post("/change_password")
def update_password(
    creds: InputUserChangePwd = Body(...),  # noqa: B008
    auth: AuthService = Depends(auth_service),  # noqa: B008
):
    token = auth.change_password(creds)
    return {"access_token": token, "token_type": "bearer"}
