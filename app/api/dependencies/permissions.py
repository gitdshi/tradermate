from fastapi import Depends, HTTPException, status
from app.api.services.auth_service import get_current_user
from app.domains.rbac.service.rbac_service import RbacService

def require_permission(resource: str, action: str):
    async def _dependency(current_user=Depends(get_current_user)):
        service = RbacService()
        if not service.check_permission(current_user.id, resource, action):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={"error": "FORBIDDEN", "message": "Permission denied", "details": {"resource": resource, "action": action}}
            )
        return current_user
    return _dependency
