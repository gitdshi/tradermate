"""TraderMate API - FastAPI Application."""
import sys
from pathlib import Path
from contextlib import asynccontextmanager
from datetime import datetime, timezone

# Ensure project root is importable
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Configure logging (ensure timestamps are present in logs)
from app.infrastructure.logging import configure_logging, get_logger  # noqa: E402
configure_logging()
logger = get_logger(__name__)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.infrastructure.config import get_settings
# Note: schema creation/migrations are handled outside the running app.
from app.api.routes import auth, strategies, data, backtest, queue
from app.api.routes import system
from app.api.routes import strategy_code
from app.domains.auth.dao.user_dao import UserDao
from app.api.services.auth_service import get_password_hash
import os

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    # Startup
    logger.info("Starting TraderMate API...")
    logger.info("Database migrations should be applied during runtime init")
    
    # Create default admin user if configured (Issue #17)
    default_username = os.getenv("DEFAULT_ADMIN_USERNAME")
    default_password = os.getenv("DEFAULT_ADMIN_PASSWORD")
    if default_username and default_password:
        try:
            user_dao = UserDao()
            if not user_dao.username_exists(default_username):
                hashed_pw = get_password_hash(default_password)
                now = datetime.now(timezone.utc)
                user_dao.insert_user(
                    username=default_username,
                    email=f"{default_username}@tradermate.local",
                    hashed_password=hashed_pw,
                    created_at=now
                )
                logger.info(f"Created default admin user '{default_username}'")
            else:
                logger.info(f"Default admin user '{default_username}' already exists, skipping creation")
        except Exception as e:
            logger.error(f"Failed to create default admin user: {e}", exc_info=True)
    else:
        logger.warning("DEFAULT_ADMIN_USERNAME or DEFAULT_ADMIN_PASSWORD not set - skipping default admin user creation")
    
    yield
    
    # Shutdown
    logger.info("Shutting down TraderMate API...")


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="TraderMate Trading Platform API - Strategy Management, Backtesting, and Market Research",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth.router, prefix="/api")
app.include_router(strategies.router, prefix="/api")
app.include_router(data.router, prefix="/api")
app.include_router(backtest.router, prefix="/api")
app.include_router(queue.router, prefix="/api")
app.include_router(system.router, prefix="/api")
app.include_router(strategy_code.router, prefix="/api")


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": settings.app_name,
        "version": settings.app_version,
        "docs": "/docs",
        "status": "running"
    }


@app.get("/health")
async def health():
    """Health check endpoint with database and Redis connectivity checks."""
    from sqlalchemy import text
    import redis
    
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "tradermate",
        "dependencies": {}
    }
    
    # Check MySQL connection
    try:
        from app.infrastructure.db.connections import get_tradermate_engine
        engine = get_tradermate_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        health_status["dependencies"]["mysql"] = {"status": "healthy"}
    except Exception as e:
        health_status["status"] = "unhealthy"
        health_status["dependencies"]["mysql"] = {"status": "unhealthy", "error": str(e)}
        logger.error(f"MySQL health check failed: {e}")
    
    # Check Redis connection
    try:
        r = redis.Redis.from_url(settings.redis_url)
        r.ping()
        health_status["dependencies"]["redis"] = {"status": "healthy"}
    except Exception as e:
        health_status["status"] = "unhealthy"
        health_status["dependencies"]["redis"] = {"status": "unhealthy", "error": str(e)}
        logger.error(f"Redis health check failed: {e}")
    
    # Return 503 if unhealthy
    from fastapi.responses import JSONResponse
    if health_status["status"] != "healthy":
        return JSONResponse(status_code=503, content=health_status)
    
    return health_status


@app.get("/api")
async def api_info():
    """API information."""
    return {
        "version": settings.app_version,
        "endpoints": {
            "auth": "/api/auth",
            "strategies": "/api/strategies",
            "backtest": "/api/backtest",
            "data": "/api/data"
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug
    )
