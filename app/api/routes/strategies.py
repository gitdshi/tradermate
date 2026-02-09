"""Strategy CRUD routes."""
from datetime import datetime
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text

from app.api.models.user import TokenData
from app.api.models.strategy import (
    Strategy,
    StrategyCreate,
    StrategyUpdate,
    StrategyListItem,
    StrategyValidation,
)
from app.api.middleware.auth import get_current_user
from app.api.services.db import get_db_connection
from app.api.services.strategy_service import validate_strategy_code

router = APIRouter(prefix="/strategies", tags=["Strategies"])


@router.get("", response_model=List[StrategyListItem])
async def list_strategies(current_user: TokenData = Depends(get_current_user)):
    """List all strategies for current user."""
    conn = get_db_connection()
    
    try:
        result = conn.execute(
            text("""
                SELECT id, name, class_name, description, version, is_active, created_at, updated_at
                FROM strategies
                WHERE user_id = :user_id
                ORDER BY updated_at DESC
            """),
            {"user_id": current_user.user_id}
        )
        rows = result.fetchall()
        
        return [
            StrategyListItem(
                id=row.id,
                name=row.name,
                class_name=row.class_name,
                description=row.description,
                version=row.version,
                is_active=row.is_active,
                created_at=row.created_at,
                updated_at=row.updated_at
            )
            for row in rows
        ]
    finally:
        conn.close()


@router.post("", response_model=Strategy, status_code=status.HTTP_201_CREATED)
async def create_strategy(
    strategy_data: StrategyCreate,
    current_user: TokenData = Depends(get_current_user)
):
    """Create a new strategy."""
    # Validate strategy code only if provided
    if strategy_data.code:
        validation = validate_strategy_code(strategy_data.code, strategy_data.class_name)
        if not validation.valid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid strategy code: {'; '.join(validation.errors)}"
            )
    
    conn = get_db_connection()
    
    try:
        # Debug logging for create attempts
        print(f"[create_strategy] user_id={current_user.user_id} name={strategy_data.name} class_name={strategy_data.class_name}")
        now = datetime.utcnow()
        
        # Check if strategy name exists for this user
        result = conn.execute(
            text("SELECT id FROM strategies WHERE user_id = :user_id AND name = :name"),
            {"user_id": current_user.user_id, "name": strategy_data.name}
        )
        if result.fetchone():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Strategy with this name already exists"
            )
        
        # Insert strategy
        import json
        result = conn.execute(
            text("""
                INSERT INTO strategies (user_id, name, class_name, description, parameters, code, version, is_active, created_at, updated_at)
                VALUES (:user_id, :name, :class_name, :description, :parameters, :code, 1, 1, :created_at, :updated_at)
            """),
            {
                "user_id": current_user.user_id,
                "name": strategy_data.name,
                "class_name": strategy_data.class_name,
                "description": strategy_data.description,
                "parameters": json.dumps(strategy_data.parameters),
                "code": strategy_data.code or "",  # Default to empty string if None
                "created_at": now,
                "updated_at": now
            }
        )
        conn.commit()

        strategy_id = result.lastrowid
        print(f"[create_strategy] inserted id={strategy_id} for user_id={current_user.user_id}")

        return Strategy(
            id=strategy_id,
            user_id=current_user.user_id,
            name=strategy_data.name,
            class_name=strategy_data.class_name,
            description=strategy_data.description,
            parameters=strategy_data.parameters,
            code=strategy_data.code or "",  # Default to empty string if None
            version=1,
            is_active=True,
            created_at=now,
            updated_at=now
        )
    finally:
        conn.close()


@router.get("/{strategy_id}", response_model=Strategy)
async def get_strategy(
    strategy_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """Get a strategy by ID."""
    conn = get_db_connection()
    
    try:
        result = conn.execute(
            text("""
                SELECT id, user_id, name, class_name, description, parameters, code, version, is_active, created_at, updated_at
                FROM strategies
                WHERE id = :strategy_id AND user_id = :user_id
            """),
            {"strategy_id": strategy_id, "user_id": current_user.user_id}
        )
        row = result.fetchone()
        
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Strategy not found"
            )
        
        import json
        return Strategy(
            id=row.id,
            user_id=row.user_id,
            name=row.name,
            class_name=row.class_name,
            description=row.description,
            parameters=json.loads(row.parameters) if row.parameters else {},
            code=row.code,
            version=row.version,
            is_active=row.is_active,
            created_at=row.created_at,
            updated_at=row.updated_at
        )
    finally:
        conn.close()


@router.put("/{strategy_id}", response_model=Strategy)
async def update_strategy(
    strategy_id: int,
    strategy_data: StrategyUpdate,
    current_user: TokenData = Depends(get_current_user)
):
    """Update a strategy."""
    conn = get_db_connection()
    
    try:
        # Check if strategy exists
        result = conn.execute(
            text("SELECT id, code, class_name FROM strategies WHERE id = :strategy_id AND user_id = :user_id"),
            {"strategy_id": strategy_id, "user_id": current_user.user_id}
        )
        existing = result.fetchone()
        
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Strategy not found"
            )
        
        # Validate name if provided (must not be empty)
        if strategy_data.name is not None and not strategy_data.name.strip():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Strategy name cannot be empty"
            )
        
        # Validate code if provided and not empty
        if strategy_data.code and strategy_data.code.strip():
            # Use the new class_name if provided, otherwise use existing
            class_name = strategy_data.class_name if strategy_data.class_name else existing.class_name
            validation = validate_strategy_code(strategy_data.code, class_name)
            if not validation.valid:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid strategy code: {'; '.join(validation.errors)}"
                )

        # If code provided, save previous code into history (keep last 5)
        if strategy_data.code is not None:
            try:
                conn2 = get_db_connection()
                prev_code = existing.code
                now = datetime.utcnow()
                conn2.execute(text("INSERT INTO strategy_code_history (strategy_id, code, created_at) VALUES (:sid, :code, :created_at)"), {"sid": strategy_id, "code": prev_code, "created_at": now})
                conn2.commit()
                # rotate to last 5
                rows = conn2.execute(text("SELECT id FROM strategy_code_history WHERE strategy_id = :sid ORDER BY created_at DESC"), {"sid": strategy_id}).fetchall()
                keep = [r.id for r in rows[:5]]
                if keep:
                    params = {"sid": strategy_id}
                    for i, v in enumerate(keep):
                        params[f"k{i}"] = v
                    conn2.execute(text(f"DELETE FROM strategy_code_history WHERE strategy_id = :sid AND id NOT IN ({','.join([':k'+str(i) for i in range(len(keep))])})"), params)
                    conn2.commit()
            except Exception:
                try:
                    conn2.close()
                except Exception:
                    pass
        
        # Build update query
        updates = []
        params = {"strategy_id": strategy_id, "user_id": current_user.user_id}
        
        # Track whether a substantive change requires version bump
        version_bump = False
        
        if strategy_data.name is not None:
            updates.append("name = :name")
            params["name"] = strategy_data.name
        if strategy_data.class_name is not None:
            updates.append("class_name = :class_name")
            params["class_name"] = strategy_data.class_name
            version_bump = True
        if strategy_data.description is not None:
            updates.append("description = :description")
            params["description"] = strategy_data.description
        if strategy_data.parameters is not None:
            import json
            updates.append("parameters = :parameters")
            params["parameters"] = json.dumps(strategy_data.parameters)
            version_bump = True
        if strategy_data.code is not None:
            updates.append("code = :code")
            params["code"] = strategy_data.code
            version_bump = True
        if strategy_data.is_active is not None:
            updates.append("is_active = :is_active")
            params["is_active"] = strategy_data.is_active
        
        # Auto-increment version when code, parameters, or class_name changes
        if version_bump:
            updates.append("version = version + 1")
        
        updates.append("updated_at = :updated_at")
        params["updated_at"] = datetime.utcnow()
        
        if updates:
            conn.execute(
                text(f"UPDATE strategies SET {', '.join(updates)} WHERE id = :strategy_id AND user_id = :user_id"),
                params
            )
            conn.commit()
        
        # Fetch updated strategy
        return await get_strategy(strategy_id, current_user)
    finally:
        conn.close()


@router.delete("/{strategy_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_strategy(
    strategy_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """Delete a strategy from database."""
    conn = get_db_connection()
    
    try:
        # Check if strategy exists
        result = conn.execute(
            text("SELECT id FROM strategies WHERE id = :strategy_id AND user_id = :user_id"),
            {"strategy_id": strategy_id, "user_id": current_user.user_id}
        )
        strategy = result.fetchone()
        
        if not strategy:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Strategy not found"
            )
        
        # Delete from database (CASCADE will handle related tables)
        conn.execute(
            text("DELETE FROM strategies WHERE id = :strategy_id AND user_id = :user_id"),
            {"strategy_id": strategy_id, "user_id": current_user.user_id}
        )
        conn.commit()
        
        print(f"[delete_strategy] Deleted strategy (id={strategy_id})")
        
    finally:
        conn.close()


@router.post("/{strategy_id}/validate", response_model=StrategyValidation)
async def validate_strategy(
    strategy_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """Validate a strategy's code."""
    strategy = await get_strategy(strategy_id, current_user)
    return validate_strategy_code(strategy.code, strategy.class_name)


@router.get("/{strategy_id}/code-history")
async def list_strategy_code_history(
    strategy_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """List stored code history for a DB strategy (latest first)."""
    conn = get_db_connection()
    try:
        # Ensure strategy belongs to current user
        owner_check = conn.execute(
            text("SELECT id FROM strategies WHERE id = :sid AND user_id = :user_id"),
            {"sid": strategy_id, "user_id": current_user.user_id}
        ).fetchone()
        if not owner_check:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Strategy not found")

        rows = conn.execute(text("SELECT id, created_at, LENGTH(code) as size FROM strategy_code_history WHERE strategy_id = :sid ORDER BY created_at DESC"), {"sid": strategy_id}).fetchall()
        out = []
        for r in rows:
            out.append({"id": r.id, "created_at": r.created_at.isoformat() if hasattr(r.created_at, 'isoformat') else str(r.created_at), "size": int(r.size)})
        return out
    finally:
        conn.close()


@router.get("/{strategy_id}/code-history/{history_id}")
async def get_strategy_code_history(
    strategy_id: int,
    history_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """Get a specific code history entry for a DB strategy."""
    conn = get_db_connection()
    try:
        # Ensure strategy belongs to current user
        owner_check = conn.execute(
            text("SELECT id FROM strategies WHERE id = :sid AND user_id = :user_id"),
            {"sid": strategy_id, "user_id": current_user.user_id}
        ).fetchone()
        if not owner_check:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Strategy not found")

        row = conn.execute(text("SELECT code FROM strategy_code_history WHERE id = :hid AND strategy_id = :sid"), {"hid": history_id, "sid": strategy_id}).fetchone()
        if not row:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="History not found")
        return {"id": history_id, "code": row.code}
    finally:
        conn.close()


@router.post("/{strategy_id}/code-history/{history_id}/restore")
async def restore_strategy_code_history(
    strategy_id: int,
    history_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """Restore a code history version to the strategy."""
    conn = get_db_connection()
    try:
        # Ensure strategy belongs to current user
        owner_check = conn.execute(
            text("SELECT id, code FROM strategies WHERE id = :sid AND user_id = :user_id"),
            {"sid": strategy_id, "user_id": current_user.user_id}
        ).fetchone()
        if not owner_check:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Strategy not found")

        # Get the history code
        history_row = conn.execute(
            text("SELECT code FROM strategy_code_history WHERE id = :hid AND strategy_id = :sid"),
            {"hid": history_id, "sid": strategy_id}
        ).fetchone()
        if not history_row:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="History not found")

        # Save current code to history before restoring
        current_code = owner_check.code
        if current_code:
            now = datetime.utcnow()
            conn.execute(
                text("INSERT INTO strategy_code_history (strategy_id, code, created_at) VALUES (:sid, :code, :created_at)"),
                {"sid": strategy_id, "code": current_code, "created_at": now}
            )

        # Restore the history code to the strategy
        conn.execute(
            text("UPDATE strategies SET code = :code, version = version + 1, updated_at = :updated_at WHERE id = :sid"),
            {"code": history_row.code, "updated_at": datetime.utcnow(), "sid": strategy_id}
        )
        conn.commit()

        return {"message": "Code history restored successfully", "strategy_id": strategy_id, "history_id": history_id}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    finally:
        conn.close()


@router.get("/builtin/list", response_model=List[StrategyListItem])
async def list_builtin_strategies():
    """List available built-in strategies."""
    # Return built-in strategies from app/strategies/
    from pathlib import Path
    import importlib.util
    
    strategies_dir = Path(__file__).resolve().parents[2] / "strategies"
    builtins = []
    
    for py_file in strategies_dir.glob("*.py"):
        if py_file.name.startswith("_") or py_file.name == "stop_loss.py":
            continue
        
        try:
            spec = importlib.util.spec_from_file_location(py_file.stem, py_file)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Find strategy classes
            for name in dir(module):
                obj = getattr(module, name)
                if isinstance(obj, type) and name.endswith("Strategy") and name != "CtaTemplate":
                    builtins.append(StrategyListItem(
                        id=0,
                        name=name,
                        class_name=name,
                        description=obj.__doc__ or f"Built-in {name}",
                        version=0,
                        is_active=True,
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow()
                    ))
        except Exception:
            continue
    
    return builtins
