"""Authentication routes."""
from fastapi import APIRouter, Depends, HTTPException, status

from app.api.config import get_settings
from app.api.models.user import UserCreate, UserLogin, User, Token, TokenData
from app.api.middleware.auth import (
    get_current_user,
)

from app.domains.auth.service import AuthService

router = APIRouter(prefix="/auth", tags=["Authentication"])
settings = get_settings()


@router.post("/register", response_model=User, status_code=status.HTTP_201_CREATED)
async def register(user_data: UserCreate):
    """Register a new user."""
    service = AuthService()
    try:
        u = service.register(user_data.username, user_data.email, user_data.password)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    return User(
        id=u["id"],
        username=u["username"],
        email=u.get("email"),
        is_active=u.get("is_active", True),
        created_at=u["created_at"],
    )


@router.post("/login", response_model=Token)
async def login(credentials: UserLogin):
    """Login and get access token."""
    service = AuthService()
    try:
        t = service.login(credentials.username, credentials.password)
    except PermissionError as e:
        msg = str(e)
        if "disabled" in msg:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=msg)
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=msg)

    return Token(
        access_token=t["access_token"],
        refresh_token=t["refresh_token"],
        token_type=t["token_type"],
        expires_in=t["expires_in"],
    )


@router.post("/refresh", response_model=Token)
async def refresh_token(refresh_token: str):
    """Refresh access token using refresh token."""
    service = AuthService()
    try:
        t = service.refresh(refresh_token)
    except PermissionError as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))

    return Token(
        access_token=t["access_token"],
        refresh_token=t["refresh_token"],
        token_type=t["token_type"],
        expires_in=t["expires_in"],
    )


@router.get("/me", response_model=User)
async def get_me(current_user: TokenData = Depends(get_current_user)):
    """Get current user info."""
    service = AuthService()
    try:
        u = service.me(current_user.user_id)
    except KeyError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    return User(
        id=u["id"],
        username=u["username"],
        email=u.get("email"),
        is_active=u.get("is_active", True),
        created_at=u["created_at"],
    )
