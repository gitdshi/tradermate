"""Strategies domain service."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
import json

from app.api.services.strategy_service import validate_strategy_code
from app.domains.strategies.dao.strategy_dao import StrategyDao
from app.domains.strategies.dao.strategy_history_dao import StrategyHistoryDao


class StrategiesService:
    def __init__(self) -> None:
        self._dao = StrategyDao()
        self._history = StrategyHistoryDao()

    def list_strategies(self, user_id: int) -> list[dict[str, Any]]:
        return self._dao.list_for_user(user_id)

    def create_strategy(
        self,
        user_id: int,
        name: str,
        class_name: str,
        description: Optional[str],
        parameters: dict[str, Any],
        code: str,
    ) -> dict[str, Any]:
        if code:
            validation = validate_strategy_code(code, class_name)
            if not validation.valid:
                raise ValueError(f"Invalid strategy code: {'; '.join(validation.errors)}")

        if self._dao.name_exists_for_user(user_id, name):
            raise ValueError("Strategy with this name already exists")

        now = datetime.utcnow()
        strategy_id = self._dao.insert_strategy(
            user_id=user_id,
            name=name,
            class_name=class_name,
            description=description,
            parameters_json=json.dumps(parameters or {}),
            code=code or "",
            created_at=now,
            updated_at=now,
        )

        return self.get_strategy(user_id, strategy_id)

    def get_strategy(self, user_id: int, strategy_id: int) -> dict[str, Any]:
        row = self._dao.get_for_user(strategy_id, user_id)
        if not row:
            raise KeyError("Strategy not found")

        try:
            params = json.loads(row.get("parameters") or "{}")
        except Exception:
            params = {}
        row["parameters"] = params
        return row

    def update_strategy(
        self,
        user_id: int,
        strategy_id: int,
        *,
        name: Optional[str] = None,
        class_name: Optional[str] = None,
        description: Optional[str] = None,
        parameters: Optional[dict[str, Any]] = None,
        code: Optional[str] = None,
        is_active: Optional[bool] = None,
    ) -> dict[str, Any]:
        existing = self._dao.get_existing_for_update(strategy_id, user_id)
        if not existing:
            raise KeyError("Strategy not found")

        # Validate name if provided
        if name is not None and not name.strip():
            raise ValueError("Strategy name cannot be empty")

        # Validate code if provided
        if code is not None and code.strip():
            cn = class_name if class_name else existing.get("class_name")
            validation = validate_strategy_code(code, cn)
            if not validation.valid:
                raise ValueError(f"Invalid strategy code: {'; '.join(validation.errors)}")

        # Determine if substantive change requires version bump and history save
        version_bump = False
        if name is not None and name != existing.get("name"):
            version_bump = True
        if description is not None and description != existing.get("description"):
            version_bump = True
        if class_name is not None and class_name != existing.get("class_name"):
            version_bump = True
        if code is not None and code != existing.get("code"):
            version_bump = True
        if parameters is not None:
            try:
                existing_params = existing.get("parameters")
                existing_parsed = json.loads(existing_params) if isinstance(existing_params, str) and existing_params else (existing_params or {})
            except Exception:
                existing_parsed = {}
            try:
                if json.dumps(existing_parsed, sort_keys=True) != json.dumps(parameters, sort_keys=True):
                    version_bump = True
            except Exception:
                version_bump = True

        # Save current state into history before update
        if version_bump:
            prev_params = existing.get("parameters")
            try:
                params_json = prev_params if isinstance(prev_params, str) else json.dumps(prev_params or {})
            except Exception:
                params_json = "{}"

            self._history.insert_history(
                strategy_id=strategy_id,
                strategy_name=existing.get("name"),
                class_name=existing.get("class_name"),
                description=existing.get("description"),
                version=existing.get("version"),
                parameters=params_json,
                code=existing.get("code"),
                created_at=datetime.utcnow(),
            )
            self._history.rotate_keep_latest(strategy_id, keep=5)

        # Build update clause
        updates: list[str] = []
        params: dict[str, Any] = {}
        if name is not None:
            updates.append("name = :name")
            params["name"] = name
        if class_name is not None:
            updates.append("class_name = :class_name")
            params["class_name"] = class_name
        if description is not None:
            updates.append("description = :description")
            params["description"] = description
        if parameters is not None:
            updates.append("parameters = :parameters")
            params["parameters"] = json.dumps(parameters)
        if code is not None:
            updates.append("code = :code")
            params["code"] = code
        if is_active is not None:
            updates.append("is_active = :is_active")
            params["is_active"] = is_active

        if version_bump:
            updates.append("version = version + 1")

        updates.append("updated_at = :updated_at")
        params["updated_at"] = datetime.utcnow()

        if updates:
            self._dao.update_strategy(strategy_id, user_id, ", ".join(updates), params)

        return self.get_strategy(user_id, strategy_id)

    def delete_strategy(self, user_id: int, strategy_id: int) -> None:
        ok = self._dao.delete_for_user(strategy_id, user_id)
        if not ok:
            raise KeyError("Strategy not found")

    def list_code_history(self, user_id: int, strategy_id: int) -> list[dict[str, Any]]:
        # Ownership check
        _ = self.get_strategy(user_id, strategy_id)
        rows = self._history.list_history(strategy_id)
        out = []
        for r in rows:
            out.append({
                "id": r.get("id"),
                "created_at": r.get("created_at").isoformat() if hasattr(r.get("created_at"), "isoformat") else str(r.get("created_at")),
                "size": int(r.get("size") or 0),
                "strategy_name": r.get("strategy_name"),
                "class_name": r.get("class_name"),
                "description": r.get("description"),
                "version": r.get("version"),
                "parameters": r.get("parameters"),
            })
        return out

    def get_code_history(self, user_id: int, strategy_id: int, history_id: int) -> dict[str, Any]:
        _ = self.get_strategy(user_id, strategy_id)
        row = self._history.get_history(strategy_id, history_id)
        if not row:
            raise KeyError("History not found")
        return {
            "id": history_id,
            "code": row.get("code"),
            "strategy_name": row.get("strategy_name"),
            "class_name": row.get("class_name"),
            "description": row.get("description"),
            "version": row.get("version"),
            "parameters": row.get("parameters"),
        }

    def restore_code_history(self, user_id: int, strategy_id: int, history_id: int) -> None:
        current = self._dao.get_existing_for_update(strategy_id, user_id)
        if not current:
            raise KeyError("Strategy not found")
        history = self._history.get_history(strategy_id, history_id)
        if not history:
            raise KeyError("History not found")

        # Save current state to history
        prev_params = current.get("parameters")
        try:
            params_json = prev_params if isinstance(prev_params, str) else json.dumps(prev_params or {})
        except Exception:
            params_json = "{}"

        if current.get("code"):
            self._history.insert_history(
                strategy_id=strategy_id,
                strategy_name=current.get("name"),
                class_name=current.get("class_name"),
                description=current.get("description"),
                version=current.get("version"),
                parameters=params_json,
                code=current.get("code"),
                created_at=datetime.utcnow(),
            )

        # Apply history values
        hist_params = history.get("parameters")
        try:
            params_val = hist_params if isinstance(hist_params, str) else json.dumps(hist_params) if hist_params is not None else None
        except Exception:
            params_val = None

        set_clause = (
            "name = :name, class_name = :class_name, description = :description, "
            "parameters = :parameters, code = :code, version = version + 1, updated_at = :updated_at"
        )
        self._dao.update_strategy(
            strategy_id,
            user_id,
            set_clause,
            {
                "name": history.get("strategy_name"),
                "class_name": history.get("class_name"),
                "description": history.get("description"),
                "parameters": params_val,
                "code": history.get("code"),
                "updated_at": datetime.utcnow(),
            },
        )
