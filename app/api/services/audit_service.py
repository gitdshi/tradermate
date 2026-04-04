"""Audit logging service."""
from typing import Optional, Any, Dict
from datetime import datetime
from sqlalchemy import text
from app.infrastructure.db.connections import get_tradermate_connection

class AuditService:
    """Logs security-relevant events."""

    def log_event(
        self,
        user_id: int,
        action: str,
        resource: str,
        resource_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> None:
        with get_tradermate_connection() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO audit_logs
                    (user_id, action, resource, resource_id, details, ip_address, user_agent, created_at)
                    VALUES (:user_id, :action, :resource, :resource_id, :details, :ip_address, :user_agent, :created_at)
                    """
                ),
                {
                    "user_id": user_id,
                    "action": action,
                    "resource": resource,
                    "resource_id": resource_id,
                    "details": str(details) if details else None,
                    "ip_address": ip_address,
                    "user_agent": user_agent,
                    "created_at": datetime.utcnow(),
                },
            )
            conn.commit()

    def log_permission_denied(
        self,
        user_id: int,
        resource: str,
        action: str,
        endpoint: str,
        method: str,
        request_id: Optional[str] = None,
    ) -> None:
        self.log_event(
            user_id=user_id,
            action="PERMISSION_DENIED",
            resource=resource,
            resource_id=None,
            details={"required_action": action, "endpoint": endpoint, "method": method, "request_id": request_id},
        )

    def log_strategy_create(self, user_id: int, strategy_id: int, name: str) -> None:
        self.log_event(user_id, "STRATEGY_CREATE", "strategy", str(strategy_id), {"name": name})

    def log_strategy_update(self, user_id: int, strategy_id: int, name: str) -> None:
        self.log_event(user_id, "STRATEGY_UPDATE", "strategy", str(strategy_id), {"name": name})

    def log_strategy_delete(self, user_id: int, strategy_id: int) -> None:
        self.log_event(user_id, "STRATEGY_DELETE", "strategy", str(strategy_id))

    def log_backtest_submit(self, user_id: int, backtest_id: int, strategy_name: str) -> None:
        self.log_event(user_id, "BACKTEST_SUBMIT", "backtest", str(backtest_id), {"strategy": strategy_name})

    def log_backtest_complete(self, user_id: int, backtest_id: int, metrics: Dict[str, Any]) -> None:
        self.log_event(user_id, "BACKTEST_COMPLETE", "backtest", str(backtest_id), metrics)

    def log_portfolio_trade(self, user_id: int, trade_id: int, symbol: str, quantity: int, price: float) -> None:
        self.log_event(user_id, "PORTFOLIO_TRADE", "trade", str(trade_id), {"symbol": symbol, "quantity": quantity, "price": price})

    def log_config_update(self, user_id: int, config_key: str, old_value: Any, new_value: Any) -> None:
        self.log_event(user_id, "CONFIG_UPDATE", "system_config", config_key, {"old": str(old_value), "new": str(new_value)})

audit_service = AuditService()
