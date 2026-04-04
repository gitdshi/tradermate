"""RBAC data access using raw SQL."""
from typing import List, Optional, Dict, Any
from sqlalchemy import text
from app.infrastructure.db.connections import get_tradermate_connection

def _get_connection():
    return get_tradermate_connection()

def get_all_roles() -> List[Dict[str, Any]]:
    with _get_connection() as conn:
        result = conn.execute(text("SELECT id, name, description, is_system, created_at, updated_at FROM roles ORDER BY id"))
        return [dict(row) for row in result.mappings()]

def get_role_by_id(role_id: int) -> Optional[Dict[str, Any]]:
    with _get_connection() as conn:
        row = conn.execute(text("SELECT id, name, description, is_system, created_at, updated_at FROM roles WHERE id = :id"), {"id": role_id}).mappings().first()
        return dict(row) if row else None

def get_role_by_name(name: str) -> Optional[Dict[str, Any]]:
    with _get_connection() as conn:
        row = conn.execute(text("SELECT id, name, description, is_system, created_at, updated_at FROM roles WHERE name = :name"), {"name": name}).mappings().first()
        return dict(row) if row else None

def create_role(name: str, description: str = None, is_system: bool = False) -> Dict[str, Any]:
    with _get_connection() as conn:
        conn.execute(text("INSERT INTO roles (name, description, is_system) VALUES (:name, :description, :is_system)"), {"name": name, "description": description, "is_system": is_system})
        conn.commit()
        role_id = conn.execute(text("SELECT LAST_INSERT_ID() as id")).scalar()
        return get_role_by_id(role_id)

def update_role(role_id: int, description: str = None, is_system: bool = None) -> Optional[Dict[str, Any]]:
    updates = []; params = {"id": role_id}
    if description is not None: updates.append("description = :description"); params["description"] = description
    if is_system is not None: updates.append("is_system = :is_system"); params["is_system"] = is_system
    if not updates: return get_role_by_id(role_id)
    with _get_connection() as conn:
        conn.execute(text(f"UPDATE roles SET {', '.join(updates)} WHERE id = :id"), params)
        conn.commit()
        return get_role_by_id(role_id)

def delete_role(role_id: int) -> bool:
    role = get_role_by_id(role_id)
    if not role: return False
    if role["is_system"]: raise ValueError("Cannot delete system role")
    with _get_connection() as conn:
        conn.execute(text("DELETE FROM role_permissions WHERE role_id = :role_id"), {"role_id": role_id})
        conn.execute(text("DELETE FROM user_roles WHERE role_id = :role_id"), {"role_id": role_id})
        conn.execute(text("DELETE FROM roles WHERE id = :role_id"), {"role_id": role_id})
        conn.commit()
        return True

def get_all_permissions() -> List[Dict[str, Any]]:
    with _get_connection() as conn:
        result = conn.execute(text("SELECT id, resource, action, description, is_system, created_at FROM permissions ORDER BY id"))
        return [dict(row) for row in result.mappings()]

def get_permission_by_id(perm_id: int) -> Optional[Dict[str, Any]]:
    with _get_connection() as conn:
        row = conn.execute(text("SELECT id, resource, action, description, is_system, created_at FROM permissions WHERE id = :id"), {"id": perm_id}).mappings().first()
        return dict(row) if row else None

def get_permission_by_resource_action(resource: str, action: str) -> Optional[Dict[str, Any]]:
    with _get_connection() as conn:
        row = conn.execute(text("SELECT id, resource, action, description, is_system, created_at FROM permissions WHERE resource = :resource AND action = :action"), {"resource": resource, "action": action}).mappings().first()
        return dict(row) if row else None

def get_permissions_for_role(role_id: int) -> List[Dict[str, Any]]:
    with _get_connection() as conn:
        result = conn.execute(text("""
            SELECT p.id, p.resource, p.action, p.description, p.is_system
            FROM permissions p
            JOIN role_permissions rp ON p.id = rp.permission_id
            WHERE rp.role_id = :role_id
            ORDER BY p.resource, p.action
        """), {"role_id": role_id})
        return [dict(row) for row in result.mappings()]

def set_role_permissions(role_id: int, permission_ids: List[int]) -> None:
    with _get_connection() as conn:
        conn.execute(text("DELETE FROM role_permissions WHERE role_id = :role_id"), {"role_id": role_id})
        for pid in permission_ids:
            conn.execute(text("INSERT INTO role_permissions (role_id, permission_id) VALUES (:role_id, :permission_id)"), {"role_id": role_id, "permission_id": pid})
        conn.commit()

def get_roles_for_user(user_id: int) -> List[Dict[str, Any]]:
    with _get_connection() as conn:
        result = conn.execute(text("""
            SELECT r.id, r.name, r.description, r.is_system
            FROM roles r
            JOIN user_roles ur ON r.id = ur.role_id
            WHERE ur.user_id = :user_id AND ur.is_active = TRUE
            ORDER BY r.id
        """), {"user_id": user_id})
        return [dict(row) for row in result.mappings()]

def get_user_ids_with_role(role_id: int) -> List[int]:
    with _get_connection() as conn:
        result = conn.execute(text("SELECT user_id FROM user_roles WHERE role_id = :role_id AND is_active = TRUE"), {"role_id": role_id})
        return [row[0] for row in result]

def assign_role_to_user(user_id: int, role_id: int, assigned_by: int = None) -> bool:
    with _get_connection() as conn:
        existing = conn.execute(text("SELECT id FROM user_roles WHERE user_id = :user_id AND role_id = :role_id"), {"user_id": user_id, "role_id": role_id}).fetchone()
        if existing:
            conn.execute(text("UPDATE user_roles SET is_active = TRUE, assigned_by = :assigned_by WHERE user_id = :user_id AND role_id = :role_id"), {"assigned_by": assigned_by, "user_id": user_id, "role_id": role_id})
        else:
            conn.execute(text("INSERT INTO user_roles (user_id, role_id, assigned_by, is_active) VALUES (:user_id, :role_id, :assigned_by, TRUE)"), {"user_id": user_id, "role_id": role_id, "assigned_by": assigned_by})
        conn.commit()
        return True

def remove_role_from_user(user_id: int, role_id: int) -> bool:
    with _get_connection() as conn:
        result = conn.execute(text("UPDATE user_roles SET is_active = FALSE WHERE user_id = :user_id AND role_id = :role_id"), {"user_id": user_id, "role_id": role_id})
        conn.commit()
        return result.rowcount > 0
