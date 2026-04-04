"""Audit middleware that logs important operations."""
from typing import Callable
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from app.api.services.audit_service import audit_service

# Operation mapping: (method, path_prefix) -> (action, resource)
OPERATION_MAP = {
    # Strategies
    ("POST", "/api/strategies"): ("STRATEGY_CREATE", "strategy"),
    ("PUT", "/api/strategies"): ("STRATEGY_UPDATE", "strategy"),
    ("DELETE", r"/api/strategies/\d+"): ("STRATEGY_DELETE", "strategy"),
    # Backtests
    ("POST", "/api/backtest"): ("BACKTEST_SUBMIT", "backtest"),
    ("DELETE", r"/api/backtest/\w+"): ("BACKTEST_CANCEL", "backtest"),
    # Portfolios
    ("POST", "/api/portfolios"): ("PORTFOLIO_CREATE", "portfolio"),
    ("PUT", "/api/portfolios"): ("PORTFOLIO_UPDATE", "portfolio"),
    ("DELETE", "/api/portfolios"): ("PORTFOLIO_DELETE", "portfolio"),
    # Data sources
    ("PUT", "/api/data-sources"): ("DATA_SOURCE_UPDATE", "data_source"),
    # Config
    ("PUT", "/api/system/configs"): ("CONFIG_UPDATE", "system_config"),
    # Paper trading
    ("POST", "/api/trading/paper/start"): ("PAPER_TRADE_START", "paper_deployment"),
    ("POST", "/api/trading/paper/stop"): ("PAPER_TRADE_STOP", "paper_deployment"),
}

class AuditMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        response = await call_next(request)
        if request.url.path.startswith("/api/audit"):
            return response

        user = getattr(request.state, "user", None)
        if not user or not hasattr(user, "id"):
            return response

        method = request.method
        path = request.url.path
        action, resource = None, None
        for (meth, pat), (act, res) in OPERATION_MAP.items():
            if meth != method:
                continue
            if pat.startswith("/") and path.startswith(pat):
                action, resource = act, res
                break

        if action and resource:
            try:
                audit_service.log_event(
                    user_id=user.id,
                    action=action,
                    resource=resource,
                    details={"method": method, "path": path},
                    ip_address=request.client.host if request.client else None,
                    user_agent=request.headers.get("user-agent", ""),
                )
            except Exception:
                pass
        return response
