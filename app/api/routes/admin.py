"""RBAC admin management routes."""
from typing import List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.api.models.user import TokenData
from app.api.dependencies.permissions import require_permission
from app.domains.rbac.dao.rbac_dao import (
    get_all_roles, get_role_by_id, get_role_by_name, create_role, update_role, delete_role,
    get_all_permissions, set_role_permissions,
    get_roles_for_user, assign_role_to_user, remove_role_from_user,
)
from app.domains.auth.dao.user_dao import UserDao

router = APIRouter(prefix="/admin", tags=["Admin"])

class RoleCreate(BaseModel):
    name: str
    description: str | None = None
    is_system: bool = False

class RoleUpdate(BaseModel):
    description: str | None = None
    is_system: bool | None = None

class PermissionAssign(BaseModel):
    permission_ids: List[int]

class UserRoleAssign(BaseModel):
    role_ids: List[int]

class UserStatusUpdate(BaseModel):
    is_active: bool

@router.get("/roles", response_model=List[Dict[str, Any]])
async def list_roles(current_user: TokenData = Depends(require_permission('account', 'manage'))):
    return get_all_roles()

@router.post("/roles", response_model=Dict[str, Any])
async def create_role(data: RoleCreate, current_user: TokenData = Depends(require_permission('account', 'manage'))):
    if get_role_by_name(data.name):
        raise HTTPException(status_code=400, detail="Role name already exists")
    return create_role(data.name, data.description, data.is_system)

@router.put("/roles/{role_id}", response_model=Dict[str, Any])
async def update_role(role_id: int, data: RoleUpdate, current_user: TokenData = Depends(require_permission('account', 'manage'))):
    role = update_role(role_id, data.description, data.is_system)
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")
    return role

@router.delete("/roles/{role_id}")
async def delete_role(role_id: int, current_user: TokenData = Depends(require_permission('account', 'manage'))):
    try:
        success = delete_role(role_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Cannot delete system role")
    if not success:
        raise HTTPException(status_code=404, detail="Role not found")
    return {"message": "Role deleted"}

@router.get("/permissions", response_model=List[Dict[str, Any]])
async def list_permissions(current_user: TokenData = Depends(require_permission('account', 'manage'))):
    return get_all_permissions()

@router.put("/roles/{role_id}/permissions")
async def set_role_permissions(role_id: int, data: PermissionAssign, current_user: TokenData = Depends(require_permission('account', 'manage'))):
    if not get_role_by_id(role_id):
        raise HTTPException(status_code=404, detail="Role not found")
    set_role_permissions(role_id, data.permission_ids)
    return {"role_id": role_id, "permission_ids": data.permission_ids}

@router.get("/users", response_model=List[Dict[str, Any]])
async def list_users(current_user: TokenData = Depends(require_permission('account', 'manage'))):
    user_dao = UserDao()
    users = user_dao.get_all_users()
    result = []
    for u in users:
        roles = get_roles_for_user(u["id"])
        result.append({
            "id": u["id"],
            "username": u["username"],
            "email": u.get("email"),
            "roles": [r["name"] for r in roles],
            "is_active": u.get("is_active", True)
        })
    return result

@router.put("/users/{user_id}/roles")
async def assign_user_roles(user_id: int, data: UserRoleAssign, current_user: TokenData = Depends(require_permission('account', 'manage'))):
    if not UserDao().user_exists(user_id):
        raise HTTPException(status_code=404, detail="User not found")
    for role in get_roles_for_user(user_id):
        remove_role_from_user(user_id, role["id"])
    for role_id in data.role_ids:
        assign_role_to_user(user_id, role_id, assigned_by=current_user.user_id)
    return {"user_id": user_id, "role_ids": data.role_ids}

@router.put("/users/{user_id}/status")
async def update_user_status(user_id: int, data: UserStatusUpdate, current_user: TokenData = Depends(require_permission('account', 'manage'))):
    success = UserDao().update_user_status(user_id, data.is_active)
    if not success:
        raise HTTPException(status_code=404, detail="User not found")
    return {"user_id": user_id, "is_active": data.is_active}
