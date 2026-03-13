"""User models for authentication."""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, EmailStr, Field


class UserBase(BaseModel):
    """Base user model."""
    username: str = Field(..., min_length=3, max_length=50)
    # Keep responses lenient to avoid failing on reserved/local domains.
    email: Optional[str] = None


class UserCreate(UserBase):
    """User creation model."""
    email: Optional[EmailStr] = None
    password: str = Field(..., min_length=6, max_length=100)


class UserLogin(BaseModel):
    """User login model."""
    username: str
    password: str


class User(UserBase):
    """User response model."""
    id: int
    is_active: bool = True
    created_at: datetime
    
    class Config:
        from_attributes = True


class UserInDB(User):
    """User model with hashed password (internal use)."""
    hashed_password: str


class Token(BaseModel):
    """JWT token response."""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int  # seconds
    must_change_password: bool = False


class TokenData(BaseModel):
    """Token payload data."""
    user_id: int
    username: str
    exp: datetime
    must_change_password: bool = False


class PasswordChangeRequest(BaseModel):
    """Request to change password."""
    current_password: str = Field(..., min_length=1, max_length=100)
    new_password: str = Field(..., min_length=6, max_length=100)
