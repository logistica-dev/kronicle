# kronicle/api/auth_routes.py
from fastapi import APIRouter, Depends
from fastapi.security import OAuth2PasswordBearer

from kronicle.auth.auth_service import AuthService
from kronicle.deps.rbac_deps import auth_service
from kronicle.schemas.rbac.user_schemas import InputUserLogin

auth_router = APIRouter(tags=["Auth"])


@auth_router.post("/login")
def login(
    user_input: InputUserLogin,
    auth: AuthService = Depends(auth_service),  # noqa: B008
):
    token = auth.login(user_input)
    return {"access_token": token, "token_type": "bearer"}


OAuth2PasswordBearer(tokenUrl="/auth/v1/login")
