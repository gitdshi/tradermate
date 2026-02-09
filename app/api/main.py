"""TraderMate API - FastAPI Application."""
import sys
from pathlib import Path
from contextlib import asynccontextmanager

# Ensure project root is importable
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.config import get_settings
from app.api.services.db import init_db
from app.api.routes import auth, strategies, data, backtest, queue

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    # Startup
    print("Starting TraderMate API...")
    try:
        init_db()
        print("Database initialized")
    except Exception as e:
        print(f"Database initialization warning: {e}")
    
    yield
    
    # Shutdown
    print("Shutting down TraderMate API...")


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
    """Health check endpoint."""
    return {"status": "healthy"}


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
