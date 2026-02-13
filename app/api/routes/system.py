"""System status routes."""
from typing import Dict, Any

from fastapi import APIRouter, Depends

from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData

from app.domains.extdata.service import SyncStatusService

router = APIRouter(prefix="/system", tags=["system"])


@router.get("/sync-status")
async def get_sync_status(
    current_user: TokenData = Depends(get_current_user)
) -> Dict[str, Any]:
    return SyncStatusService().get_sync_status()
