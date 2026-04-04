from typing import Set
from app.domains.rbac.dao.rbac_dao import get_roles_for_user, get_permissions_for_role

class RbacService:
    def get_user_permissions(self, user_id: int) -> Set[str]:
        roles = get_roles_for_user(user_id)
        perms = set()
        for role in roles:
            role_perms = get_permissions_for_role(role["id"])
            for p in role_perms:
                perms.add(f"{p['resource']}.{p['action']}")
        return perms

    def check_permission(self, user_id: int, resource: str, action: str) -> bool:
        required = f"{resource}.{action}"
        return required in self.get_user_permissions(user_id)

    def user_has_role(self, user_id: int, role_name: str) -> bool:
        roles = get_roles_for_user(user_id)
        return any(r["name"] == role_name for r in roles)
