# TraderMate 环境变量清单

## 概述

本文档列出 TraderMate 应用所需的全部环境变量及其说明。

## 必需变量

### 安全配置

| 变量名 | 说明 | 验证要求 | 默认值 |
|--------|------|----------|--------|
| `SECRET_KEY` | JWT 签名密钥 | 至少 32 字符，随机生成 | 无（必填） |
| `MYSQL_PASSWORD` | MySQL root 密码 | 非空 | 无（必填） |
| `TUSHARE_TOKEN` | Tushare API token | 有效 token | 无（必填） |

### 数据库连接

| 变量名 | 说明 | 默认值 |
|--------|------|--------|
| `MYSQL_HOST` | MySQL 主机地址 | `mysql` (Docker) / `127.0.0.1` (本地) |
| `MYSQL_PORT` | MySQL 端口 | `3306` |
| `MYSQL_USER` | MySQL 用户名 | `root` |
| `MYSQL_DATABASE` | 主数据库名 | `tushare` |

### Redis 配置

| 变量名 | 说明 | 默认值 |
|--------|------|--------|
| `REDIS_HOST` | Redis 主机地址 | `redis` (Docker) / `127.0.0.1` (本地) |
| `REDIS_PORT` | Redis 端口 | `6379` |
| `REDIS_DB` | Redis 数据库索引 | `0` |

### JWT 配置

| 变量名 | 说明 | 默认值 |
|--------|------|--------|
| `JWT_ALGORITHM` | JWT 算法 | `HS256` |
| `ACCESS_TOKEN_EXPIRE_MINUTES` | Access token 有效期（分钟） | `1440` (24小时) |
| `REFRESH_TOKEN_EXPIRE_DAYS` | Refresh token 有效期（天） | `7` |

### 应用配置

| 变量名 | 说明 | 默认值 |
|--------|------|--------|
| `APP_NAME` | 应用名称 | `TraderMate API` |
| `APP_VERSION` | 应用版本 | `1.0.0` |
| `DEBUG` | 调试模式 | `False` |
| `CORS_ORIGINS` | 允许的跨域源（逗号分隔） | `http://localhost:5173,http://localhost:3000` |

## 可选变量

| 变量名 | 说明 | 默认值 |
|--------|------|--------|
| `SYNC_INTERVAL_HOURS` | 数据同步间隔（小时） | `24` |
| `BATCH_SIZE` | 批量处理大小 | `100` |
| `MAX_RETRIES` | 最大重试次数 | `3` |
| `LOG_LEVEL` | 日志级别 | `INFO` |

## 使用方式

### 本地开发

```bash
cp .env.example .env
# 编辑 .env，填入真实值
```

### Docker Compose

```bash
# 方式1: 使用 .env 文件（自动加载）
docker-compose up -d

# 方式2: 手动指定
export MYSQL_PASSWORD=yourpassword
export SECRET_KEY=$(openssl rand -hex 32)
export TUSHARE_TOKEN=yourtoken
docker-compose up -d
```

### 生产环境

建议使用 Docker secrets 或外部配置管理（如 Kubernetes Secrets、Vault）。

**Docker secrets 示例**：

```bash
echo "your-mysql-password" | docker secret create mysql_password -
echo "your-jwt-secret" | docker secret create jwt_secret -
# 在 docker-compose.yml 中使用 secrets 引用
```

## 安全建议

1. ✅ **永远不要提交 `.env` 到版本控制**（已在 `.gitignore` 中）
2. ✅ **使用强密码**（至少 32 字符随机字符串）
3. ✅ **生产环境使用 secrets 管理**，避免环境文件
4. ✅ **定期轮换密钥**
5. ✅ **限制 `.env.example` 只包含占位符，不包含真实值**

---

最后更新: 2026-02-26
