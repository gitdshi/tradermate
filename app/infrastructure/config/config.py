"""
TraderMate 配置模块
从环境变量加载配置，确保敏感信息不硬编码
"""

import os
from typing import Optional
from dataclasses import dataclass, field


@dataclass
class DatabaseConfig:
    """数据库配置"""
    host: str = os.getenv("MYSQL_HOST", "localhost")
    port: int = int(os.getenv("MYSQL_PORT", "3306"))
    database: str = os.getenv("MYSQL_DATABASE", "tradermate")
    username: str = os.getenv("MYSQL_USER", "tradermate")
    password: str = os.getenv("MYSQL_PASSWORD", "")
    root_password: str = os.getenv("MYSQL_ROOT_PASSWORD", "")

    @property
    def url(self) -> str:
        """构建数据库连接 URL"""
        return f"mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

    def validate(self) -> list[str]:
        """验证配置完整性"""
        errors = []
        if not self.password:
            errors.append("MYSQL_PASSWORD 环境变量未设置")
        if not self.database:
            errors.append("MYSQL_DATABASE 未配置")
        return errors


@dataclass
class JWTConfig:
    """JWT 配置"""
    secret_key: str = os.getenv("JWT_SECRET_KEY", "")
    algorithm: str = os.getenv("JWT_ALGORITHM", "HS256")
    access_token_expire_minutes: int = int(os.getenv("JWT_EXPIRE_MINUTES", "30"))

    def validate(self) -> list[str]:
        """验证配置完整性"""
        errors = []
        if not self.secret_key:
            errors.append("JWT_SECRET_KEY 环境变量未设置")
        return errors


@dataclass
class TushareConfig:
    """Tushare API 配置"""
    token: str = os.getenv("TUSHARE_TOKEN", "")

    def validate(self) -> list[str]:
        """验证配置完整性"""
        errors = []
        if not self.token:
            errors.append("TUSHARE_TOKEN 环境变量未设置")
        return errors


@dataclass
class RedisConfig:
    """Redis 配置"""
    url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    host: str = os.getenv("REDIS_HOST", "localhost")
    port: int = int(os.getenv("REDIS_PORT", "6379"))
    db: int = int(os.getenv("REDIS_DB", "0"))

    def validate(self) -> list[str]:
        """验证配置完整性"""
        errors = []
        if not self.url:
            errors.append("REDIS_URL 未配置")
        return errors


@dataclass
class AppConfig:
    """应用主配置"""
    environment: str = os.getenv("ENVIRONMENT", "production")
    debug: bool = os.getenv("DEBUG", "False").lower() in ("true", "1", "yes")
    api_port: int = int(os.getenv("API_PORT", "8000"))

    # 子配置
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    jwt: JWTConfig = field(default_factory=JWTConfig)
    tushare: TushareConfig = field(default_factory=TushareConfig)
    redis: RedisConfig = field(default_factory=RedisConfig)

    def validate_all(self) -> list[str]:
        """验证所有配置"""
        errors = []
        errors.extend(self.database.validate())
        errors.extend(self.jwt.validate())
        errors.extend(self.tushare.validate())
        errors.extend(self.redis.validate())
        return errors

    def ensure_valid(self) -> None:
        """确保配置有效，无效时抛出异常"""
        errors = self.validate_all()
        if errors:
            error_msg = "配置验证失败，缺失必需的环境变量:\n" + "\n".join(f"  - {e}" for e in errors)
            raise RuntimeError(error_msg)


# 全局配置单例
config = AppConfig()

# 启动时自动验证（可选，推荐在应用启动时调用）
if __name__ == "__main__":
    try:
        config.ensure_valid()
        print("✅ 配置验证通过")
    except RuntimeError as e:
        print(f"❌ {e}")
        exit(1)
