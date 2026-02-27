"""Application settings moved to infrastructure package.

This module is a copy of `app.api.config` moved to
`app.infrastructure.config.config` to centralize runtime configuration.
"""
import os
from datetime import timedelta
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore"
    )

    app_name: str = "TraderMate API"
    app_version: str = "1.0.0"
    debug: bool = False

    secret_key: str  # JWT 密钥，必须从环境变量 SECRET_KEY 读取
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60 * 24
    refresh_token_expire_days: int = 7

    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "root"
    mysql_password: str  # 必须从环境变量 MYSQL_PASSWORD 读取
    tushare_db: str = "tushare"
    tradermate_db: str = "tradermate"

    redis_host: str = "127.0.0.1"
    redis_port: int = 6379
    redis_db: int = 0

    cors_origins: list[str] = ["http://localhost:5173", "http://localhost:3000", "http://127.0.0.1:5173"]

    max_concurrent_backtests: int = 4
    backtest_timeout_seconds: int = 600

    @property
    def mysql_url(self) -> str:
        return f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}@{self.mysql_host}:{self.mysql_port}"

    @property
    def tushare_db_url(self) -> str:
        return f"{self.mysql_url}/{self.tushare_db}?charset=utf8mb4"

    @property
    def tradermate_db_url(self) -> str:
        return f"{self.mysql_url}/{self.tradermate_db}?charset=utf8mb4"

    @property
    def vnpy_db_url(self) -> str:
        return self.tradermate_db_url

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
