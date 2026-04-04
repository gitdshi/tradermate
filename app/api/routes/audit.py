"""Audit log routes."""
from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from app.api.models.user import TokenData
from app.api.dependencies.permissions import require_permission
from app.infrastructure.db.connections import get_tradermate_connection
from sqlalchemy import text

router = APIRouter(prefix="/audit", tags=["Audit"])

class AuditLogEntry(BaseModel):
    id: int
    user_id: int
    action: str
    resource: str
    resource_id: Optional[str]
    details: Optional[str]
    ip_address: Optional[str]
    created_at: datetime

@router.get("/logs", response_model=List[AuditLogEntry])
async def get_audit_logs(
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    user_id: Optional[int] = Query(None),
    action: Optional[str] = Query(None),
    resource: Optional[str] = Query(None),
    limit: int = Query(100, le=1000),
    current_user: TokenData = Depends(require_permission('system', 'manage'))
):
    with get_tradermate_connection() as conn:
        query = "SELECT id, user_id, action, resource, resource_id, details, ip_address, created_at FROM audit_logs WHERE 1=1"
        params = {}
        if start_date:
            query += " AND created_at >= :start_date"
            params["start_date"] = start_date
        if end_date:
            query += " AND created_at <= :end_date"
            params["end_date"] = end_date
        if user_id:
            query += " AND user_id = :user_id"
            params["user_id"] = user_id
        if action:
            query += " AND action = :action"
            params["action"] = action
        if resource:
            query += " AND resource = :resource"
            params["resource"] = resource
        query += " ORDER BY created_at DESC LIMIT :limit"
        params["limit"] = limit

        result = conn.execute(text(query), params)
        rows = result.mappings().all()
        return [dict(row) for row in rows]
