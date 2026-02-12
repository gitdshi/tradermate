"""Strategy CRUD routes."""
from datetime import datetime
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status

from app.api.models.user import TokenData
from app.api.models.strategy import (
    Strategy,
    StrategyCreate,
    StrategyUpdate,
    StrategyListItem,
    StrategyValidation,
)
from app.api.middleware.auth import get_current_user
from app.api.services.strategy_service import validate_strategy_code

from app.domains.strategies.service import StrategiesService

router = APIRouter(prefix="/strategies", tags=["Strategies"])


@router.get("", response_model=List[StrategyListItem])
async def list_strategies(current_user: TokenData = Depends(get_current_user)):
    """List all strategies for current user."""
    service = StrategiesService()
    rows = service.list_strategies(current_user.user_id)
    return [
        StrategyListItem(
            id=r["id"],
            name=r["name"],
            class_name=r.get("class_name"),
            description=r.get("description"),
            version=r.get("version"),
            is_active=r.get("is_active"),
            created_at=r.get("created_at"),
            updated_at=r.get("updated_at"),
        )
        for r in rows
    ]


@router.post("", response_model=Strategy, status_code=status.HTTP_201_CREATED)
async def create_strategy(
    strategy_data: StrategyCreate,
    current_user: TokenData = Depends(get_current_user)
):
    """Create a new strategy."""
    service = StrategiesService()
    try:
        row = service.create_strategy(
            user_id=current_user.user_id,
            name=strategy_data.name,
            class_name=strategy_data.class_name,
            description=strategy_data.description,
            parameters=strategy_data.parameters or {},
            code=strategy_data.code or "",
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    return Strategy(
        id=row["id"],
        user_id=row["user_id"],
        name=row["name"],
        class_name=row.get("class_name"),
        description=row.get("description"),
        parameters=row.get("parameters") or {},
        code=row.get("code") or "",
        version=row.get("version") or 1,
        is_active=bool(row.get("is_active")),
        created_at=row.get("created_at") or datetime.utcnow(),
        updated_at=row.get("updated_at") or datetime.utcnow(),
    )


@router.get("/{strategy_id}", response_model=Strategy)
async def get_strategy(
    strategy_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """Get a strategy by ID."""
    service = StrategiesService()
    try:
        row = service.get_strategy(current_user.user_id, strategy_id)
    except KeyError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Strategy not found")

    return Strategy(
        id=row["id"],
        user_id=row["user_id"],
        name=row["name"],
        class_name=row.get("class_name"),
        description=row.get("description"),
        parameters=row.get("parameters") or {},
        code=row.get("code") or "",
        version=row.get("version") or 1,
        is_active=bool(row.get("is_active")),
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
    )


@router.put("/{strategy_id}", response_model=Strategy)
async def update_strategy(
    strategy_id: int,
    strategy_data: StrategyUpdate,
    current_user: TokenData = Depends(get_current_user)
):
    """Update a strategy."""
    service = StrategiesService()
    try:
        row = service.update_strategy(
            current_user.user_id,
            strategy_id,
            name=strategy_data.name,
            class_name=strategy_data.class_name,
            description=strategy_data.description,
            parameters=strategy_data.parameters,
            code=strategy_data.code,
            is_active=strategy_data.is_active,
        )
    except KeyError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Strategy not found")
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    return Strategy(
        id=row["id"],
        user_id=row["user_id"],
        name=row["name"],
        class_name=row.get("class_name"),
        description=row.get("description"),
        parameters=row.get("parameters") or {},
        code=row.get("code") or "",
        version=row.get("version") or 1,
        is_active=bool(row.get("is_active")),
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
    )


@router.delete("/{strategy_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_strategy(
    strategy_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """Delete a strategy from database."""
    service = StrategiesService()
    try:
        service.delete_strategy(current_user.user_id, strategy_id)
    except KeyError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Strategy not found")


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
    service = StrategiesService()
    try:
        return service.list_code_history(current_user.user_id, strategy_id)
    except KeyError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Strategy not found")


@router.get("/{strategy_id}/code-history/{history_id}")
async def get_strategy_code_history(
    strategy_id: int,
    history_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """Get a specific code history entry for a DB strategy."""
    service = StrategiesService()
    try:
        return service.get_code_history(current_user.user_id, strategy_id, history_id)
    except KeyError as e:
        msg = str(e)
        if "History" in msg:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="History not found")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Strategy not found")


@router.post("/{strategy_id}/code-history/{history_id}/restore")
async def restore_strategy_code_history(
    strategy_id: int,
    history_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """Restore a code history version to the strategy."""
    service = StrategiesService()
    try:
        service.restore_code_history(current_user.user_id, strategy_id, history_id)
        return {"message": "Code history restored successfully", "strategy_id": strategy_id, "history_id": history_id}
    except KeyError as e:
        msg = str(e)
        if "History" in msg:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="History not found")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Strategy not found")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


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
