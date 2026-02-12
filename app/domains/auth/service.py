"""Auth domain service."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from app.api.config import get_settings
from app.api.middleware.auth import (
    create_access_token,
    create_refresh_token,
    decode_token,
    get_password_hash,
    verify_password,
)
from app.domains.auth.dao.user_dao import UserDao

settings = get_settings()


class AuthService:
    def __init__(self) -> None:
        self._users = UserDao()

    def register(self, username: str, email: Optional[str], password: str) -> dict:
        if self._users.username_exists(username):
            raise ValueError("Username already registered")
        if email and self._users.email_exists(email):
            raise ValueError("Email already registered")

        now = datetime.utcnow()
        user_id = self._users.insert_user(username, email, get_password_hash(password), now)
        return {
            "id": user_id,
            "username": username,
            "email": email,
            "is_active": True,
            "created_at": now,
        }

    def login(self, username: str, password: str) -> dict:
        user = self._users.get_user_for_login(username)
        if not user:
            raise PermissionError("Incorrect username or password")
        if not verify_password(password, user["hashed_password"]):
            raise PermissionError("Incorrect username or password")
        if not user.get("is_active"):
            raise PermissionError("User account is disabled")

        access_token = create_access_token(user["id"], user["username"])
        refresh_token = create_refresh_token(user["id"], user["username"])
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
            "expires_in": settings.access_token_expire_minutes * 60,
        }

    def refresh(self, refresh_token: str) -> dict:
        token_data = decode_token(refresh_token)
        if token_data is None:
            raise PermissionError("Invalid or expired refresh token")

        user = self._users.get_user_by_id(token_data.user_id)
        if not user or not user.get("is_active"):
            raise PermissionError("User not found or inactive")

        new_access_token = create_access_token(user["id"], user["username"])
        new_refresh_token = create_refresh_token(user["id"], user["username"])
        return {
            "access_token": new_access_token,
            "refresh_token": new_refresh_token,
            "token_type": "bearer",
            "expires_in": settings.access_token_expire_minutes * 60,
        }

    def me(self, user_id: int) -> dict:
        user = self._users.get_user_by_id(user_id)
        if not user:
            raise KeyError("User not found")
        return user
