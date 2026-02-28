# TraderMate 系统架构文档

**版本**: 1.0.0  
**最后更新**: 2026-02-28  
**作者**: TraderMate 项目组

---

## 一、系统概览

TraderMate 是一个现代化的个人量化投资平台，采用 **微服务架构** 设计，支持策略开发、回测、数据管理和实时交易。

### 核心设计原则

- **前后端分离**: 前端独立部署，通过 REST API 与后端通信
- **领域驱动设计 (DDD)**: 后端按业务域划分模块
- **容器化部署**: 基于 Docker Compose 的一键部署
- **异步任务处理**: 使用 Redis RQ 处理耗时的回测任务
- **多数据源支持**: 集成 Tushare、AkShare 等国内数据源

### 技术栈总览

| 层级 | 技术选型 | 说明 |
|------|---------|------|
| **前端** | React 19 + TypeScript + Vite | 现代化 Web 界面 |
| **UI 框架** | Tailwind CSS | 原子化 CSS |
| **状态管理** | Zustand | 轻量级状态管理 |
| **图表** | Recharts + Plotly | 数据可视化 |
| **代码编辑器** | Monaco Editor | 策略代码编辑 |
| **后端** | Python 3.11 + FastAPI | 高性能异步框架 |
| **ORM/SQL** | SQLAlchemy Core + PyMySQL | 数据库访问 |
| **任务队列** | Redis + RQ | 后台任务处理 |
| **数据库** | MySQL 8.0 | 主数据存储 |
| **缓存/消息** | Redis 7 | 缓存和队列 |
| **容器化** | Docker + Docker Compose | 部署和编排 |
| **量化引擎** | VN.PY (vnpy) | 策略回测和执行 |
| **数据源** | Tushare, AkShare, yfinance | 市场数据 |

---

## 二、系统架构图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                            用户层 (Browser)                              │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │                    TraderMate Portal (React)                      │ │
│  │  • 策略管理  • 回测引擎  • 数据可视化  • 代码编辑  • 用户认证        │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────┬──────────────────────────────────┘
                                       │ HTTPS / HTTP (CORS enabled)
┌─────────────────────────────────────────────────────────────────────────┐
│                            网关/API 层                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │                 FastAPI Application (Port 8000)                   │ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐         │ │
│  │  │   Auth   │  │ Strategy │  │ Backtest │  │   Data   │         │ │
│  │  │  Routes  │  │  Routes  │  │  Routes  │  │  Routes  │         │ │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘         │ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐                            │ │
│  │  │  Queue   │  │  System  │  │Strategy  │                            │ │
│  │  │ Routes   │  │ Routes   │  │  Code    │                            │ │
│  │  └──────────┘  └──────────┘  └──────────┘                            │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────┬──────────────────────────────────┘
                                       │
┌─────────────────────────────────────────────────────────────────────────┐
│                            业务服务层                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │                     RQ Worker (Background)                        │ │
│  │  • 执行回测任务  • 参数优化  • 批量计算                              │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │                  DataSync Daemon (Scheduler)                      │ │
│  │  • 市场数据同步  • 增量更新  • 缺失日期检测                          │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │                     VN.PY Desktop (Optional)                      │ │
│  │  • 实时交易  • 策略监控  • 手动干预                                │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────┬──────────────────────────────────┘
                                       │
┌─────────────────────────────────────────────────────────────────────────┐
│                            数据层                                       │
│  ┌──────────────┐        ┌──────────────┐        ┌──────────────┐    │
│  │    MySQL     │◄──────►│    Redis     │◄──────►│   RQ Queue   │    │
│  │   (8.0)      │        │    7.0       │        │              │    │
│  │              │        │              │        │              │    │
│  │ • tushare    │        │ • 缓存        │        │ • backtest   │    │
│  │ • tradermate │        │ • 会话        │        │ • optimization│   │
│  │ • vnpy       │        │ • 任务锁      │        │              │    │
│  └──────────────┘        └──────────────┘        └──────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 三、组件详解

### 3.1 前端 (tradermate-portal)

#### 技术栈
- **React 19**: 利用最新并发特性提升用户体验
- **TypeScript**: 类型安全，提升代码质量
- **Vite**: 极速开发服务器和构建工具
- **Tailwind CSS**: 原子化 CSS，快速构建响应式界面
- **Zustand**: 轻量全局状态管理
- **React Query**: 服务端状态管理和缓存
- **Monaco Editor**: VS Code 同款代码编辑器
- **Recharts**: 策略绩效图表
- **React Router**: 客户端路由

#### 核心功能模块

| 模块 | 路径 | 职责 |
|------|------|------|
| **认证** | `src/pages/Login.tsx` | 用户登录/登出，JWT 管理 |
| **策略管理** | `src/pages/Strategies.tsx` | 策略 CRUD，版本管理 |
| **回测** | `src/pages/Backtest.tsx` | 单次回测配置与运行 |
| **批量回测** | `src/pages/BatchBacktest.tsx` | 多策略/多参数批量回测 |
| **结果分析** | `src/pages/Results.tsx` | 回测绩效图表展示 |
| **数据管理** | `src/pages/Data.tsx` | 市场数据查看与同步 |
| **系统状态** | `src/pages/System.tsx` | 服务健康状态监控 |

#### 状态管理 (Stores)

- **auth.store.ts**: 用户认证状态、JWT token
- **strategy.store.ts**: 策略列表、当前策略
- **backtest.store.ts**: 回测任务状态、结果缓存

---

### 3.2 后端 API (tradermate)

#### 技术栈
- **FastAPI**: 高性能异步 Web 框架，自动 OpenAPI 文档
- **Pydantic v2**: 数据验证和设置管理
- **SQLAlchemy 2.0**: SQL 工具包（使用 Core，非 ORM）
- **Uvicorn**: ASGI 服务器
- **Python-Jose**: JWT 令牌管理
- **Passlib + Bcrypt**: 密码哈希

#### 架构分层

```
app/
├── api/
│   ├── main.py              # FastAPI 应用入口
│   ├── routes/              # 路由层 ( routers )
│   │   ├── auth.py          # 认证相关 (/api/auth)
│   │   ├── strategies.py    # 策略 CRUD (/api/strategies)
│   │   ├── backtest.py      # 回测任务 (/api/backtest)
│   │   ├── data.py          # 数据接口 (/api/data)
│   │   ├── queue.py         # 任务队列状态 (/api/queue)
│   │   ├── system.py        # 系统监控 (/api/system)
│   │   ├── strategy_code.py # 策略代码验证 (/api/strategy_code)
│   │   └── models/          # Pydantic 请求/响应模型
│   └── services/            # 应用服务层
│       ├── auth_service.py
│       ├── strategy_service.py
│       ├── backtest_service.py
│       └── job_storage_service.py
├── domains/                 # 业务域 (DDD)
│   ├── auth/
│   │   └── service.py       # 用户认证逻辑
│   ├── strategies/
│   │   ├── service.py       # 策略业务逻辑
│   │   └── dao/             # 数据访问对象
│   ├── backtests/
│   │   ├── service.py       # 回测执行逻辑
│   │   └── dao/             # 回测历史 DAO
│   ├── extdata/             # 外部数据管理
│   │   ├── service.py       # 数据同步服务
│   │   └── dao/             # 数据 DAO (tushare, akshare, vnpy)
│   ├── market/
│   │   ├── service.py       # 市场数据服务
│   │   └── dao/             # 市场数据 DAO
│   └── jobs/
│       └── service.py       # 任务调度
├── infrastructure/          # 基础设施层
│   ├── config/config.py    # 应用配置 (Pydantic Settings)
│   ├── db/connections.py   # 数据库连接管理
│   └── logging/            # 日志配置
├── datasync/               # 数据同步模块
│   ├── service/
│   │   ├── data_sync_daemon.py  # 同步调度器
│   │   ├── tushare_ingest.py    # Tushare 数据摄入
│   │   └── akshare_ingest.py    # AkShare 数据摄入
│   └── tasks/              # Celery/RQ 任务
├── worker/
│   └── service/
│       └── tasks.py        # RQ 任务定义 (回测任务)
└── app.py                  # 旧入口 (保留兼容)

```

#### 核心 Domain 服务

** StrategiesService (策略域)**
- `list_strategies(user_id)` - 列出用户策略
- `create_strategy(...)` - 创建策略
- `get_strategy(id, user_id)` - 获取策略详情
- `update_strategy(...)` - 更新策略
- `delete_strategy(id, user_id)` - 删除策略
- `validate_strategy_code(code)` - 语法检查

**BacktestService (回测域)**
- `submit_backtest(request, user_id)` - 提交单次回测
- `submit_batch_backtest(request, user_id)` - 提交批量回测
- `get_backtest_result(job_id)` - 获取回测结果
- `list_backtest_history(user_id)` - 回测历史列表
- `export_backtest_result(job_id)` - 导出结果 (CSV/JSON)

**ExtdataService (外部数据域)**
- `sync_tushare_data(start_date, end_date)` - 同步 Tushare 数据
- `sync_akshare_data(symbol)` - 同步 AkShare 数据
- `get_sync_status()` - 获取同步状态
- `find_missing_trade_dates(lookback_days)` - 检测缺失交易日

---

### 3.3 数据库设计

#### 数据库实例

TraderMate 使用 **单一 MySQL 实例**，但包含多个业务数据库：

| 数据库名 | 用途 | 主要表 |
|---------|------|-------|
| `tushare` | Tushare 市场数据 | `trade_calendar`, `stock_daily`, `index_daily`, `fund_daily`, ... |
| `tradermate` | 应用业务数据 | `strategies`, `backtest_history`, `bulk_backtests`, `bulk_results`, `sync_log`, `sync_status`, `users` |
| `vnpy` | VN.PY 交易数据 | `dbbar`, `dborder`, `dbtrade`, `dbposition`, ... |

#### Tradermate 数据库核心表

##### `strategies` (策略表)

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INT PK | 主键 |
| `user_id` | INT | 用户 ID (外键) |
| `name` | VARCHAR(255) | 策略名称 |
| `class_name` | VARCHAR(255) | 策略类名 (用于 VN.PY 加载) |
| `description` | TEXT | 策略描述 |
| `parameters` | JSON | 参数配置 (JSON 字符串) |
| `code` | LONGTEXT | 策略源代码 (Python) |
| `version` | INT | 版本号 (乐观锁) |
| `is_active` | TINYINT(1) | 是否启用 |
| `created_at` | DATETIME | 创建时间 |
| `updated_at` | DATETIME | 更新时间 |

##### `backtest_history` (回测历史)

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INT PK | 主键 |
| `user_id` | INT | 用户 ID |
| `strategy_id` | INT | 关联策略 ID |
| `job_id` | VARCHAR(36) | 回测任务 UUID |
| `status` | ENUM | `pending/running/completed/failed` |
| `parameters` | JSON | 回测参数 |
| `result_summary` | JSON | 结果摘要 (Sharpe, MaxDD, etc.) |
| `result_data` | JSON/LONGTEXT | 完整结果 (权益曲线、交易记录) |
| `started_at` | DATETIME | 开始时间 |
| `completed_at` | DATETIME | 完成时间 |

##### `bulk_backtests` (批量回测任务)

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INT PK | 主键 |
| `user_id` | INT | 用户 ID |
| `batch_id` | VARCHAR(36) | 批次 UUID (前端生成) |
| `total_jobs` | INT | 总任务数 |
| `completed_jobs` | INT | 已完成数 |
| `failed_jobs` | INT | 失败数 |
| `status` | ENUM | 批次状态 |
| `created_at` | DATETIME | 创建时间 |

##### `sync_log` (数据同步日志)

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INT PK | 主键 |
| `source` | VARCHAR(50) | 数据源 (`tushare`, `akshare`) |
| `symbol` | VARCHAR(50) | 证券代码 |
| `exchange` | VARCHAR(20) | 交易所 |
| `interval` | VARCHAR(10) | 周期 (`D`, `1min`, ...) |
| `start_date` | DATE | 开始日期 |
| `end_date` | DATE | 结束日期 |
| `rows_inserted` | INT | 插入行数 |
| `status` | ENUM | `success/failed/partial` |
| `error_message` | TEXT | 错误信息 |
| `created_at` | DATETIME | 同步时间 |

##### `sync_status` (同步状态表)

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INT PK | 主键 |
| `source` | VARCHAR(50) | 数据源 |
| `symbol` | VARCHAR(50) | 证券代码 |
| `exchange` | VARCHAR(20) | 交易所 |
| `interval` | VARCHAR(10) | 周期 |
| `last_sync_end` | DATE | 最后同步结束日期 |
| `next_sync_start` | DATE | 下一次同步开始日期 |
| `is_active` | TINYINT(1) | 是否启用自动同步 |

---

### 3.4 消息队列与异步任务

#### Redis + RQ

- **Redis 用途**:
  - RQ 任务队列存储 (`tradermate` 队列)
  - 回测任务状态缓存
  - 用户会话缓存 (可选)
  - 分布式锁 (用于数据同步)

- **任务类型**:
  - `backtest`: 单次回测任务
  - `optimization`: 参数优化任务
  - `data_sync`: 数据同步任务

#### Worker 服务

```bash
rq worker --url redis://redis:6379 backtest optimization
```

- **并发控制**: `max_concurrent_backtests` (配置中限制为 4)
- **超时设置**: 默认 600 秒 (可配置)
- **结果存储**: 回测结果写回 MySQL `backtest_history` 表

---

## 四、数据流

### 4.1 用户认证流程

```
┌─────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────┐
│  前端   │────▶│  POST /auth │────▶│ AuthService │────▶│  MySQL  │
│         │◀────│   /login    │◀────│             │◀────│  users  │
└─────────┘     └─────────────┘     └─────────────┘     └─────────┘
      │                                          │
      │                                  验证密码 (bcrypt)
      │                                          │
      │                                   生成 JWT
      │                                          │
      └──────────────────────────────────────────┘
                   返回 { access_token, refresh_token }
```

1. 用户提交凭据 `POST /api/auth/login`
2. `AuthService` 查询 `users` 表验证密码
3. 生成 JWT access token (有效期 30 分钟) 和 refresh token (7 天)
4. 前端存储 token，后续请求通过 `Authorization: Bearer` 头传递

### 4.2 策略 CRUD 流程

```
┌─────────┐     ┌─────────────┐     ┌──────────────┐     ┌─────────┐
│  前端   │────▶│  Strategy   │────▶│  Strategy    │────▶│  MySQL  │
│         │◀────│   Service   │◀────│     DAO      │◀────│strategies│
└─────────┘     └─────────────┘     └──────────────┘     └─────────┘
      │                                   │
      │                          代码验证 (AST parse)
      │                                   │
      │                          保存源码和参数
      │                                   │
      └───────────────────────────────────┘
```

**代码验证**: 提交策略代码时，`validate_strategy_code()` 会解析 AST 检查：
- 必须是有效的 Python 语法
- 必须定义继承自 `vnpy_ctastrategy.CtaTemplate` 的类
- 必须包含 `on_init`, `on_bar` 等必要方法

### 4.3 回测任务提交流程

```
┌─────────┐     ┌─────────────┐     ┌──────────────┐     ┌─────────┐
│  前端   │────▶│  Backtest   │────▶│  Backtest    │────▶│   RQ    │
│         │     │   Service   │     │    Service   │     │  Queue  │
└─────────┘     └─────────────┘     └──────────────┘     └─────────┘
      │                  │                       │
      │             生成 job_id                  │
      │                  │                       │
      │           存入内存 jobs dict             │
      │                  │                       │
      │            background_tasks             │
      │                  │                       │
      │                  └─────────────┬─────────┘
      │                                │
┌─────────┐                    ┌─────────────┐
│  前端   │◀───────────────────│  Worker     │
│ (轮询)  │    状态更新         │  (RQ)       │
└─────────┘                    └─────────────┘
                                   │
                                   ▼
                             ┌─────────┐
                             │  VN.PY  │
                             │ Backtest│
                             └─────────┘
                                   │
                                   ▼
                            写入 MySQL backtest_history
```

1. 前端提交 `POST /api/backtest`，携带策略 ID、参数、回测周期
2. `BacktestService` 生成 `job_id` (UUID)，存入内存 `_jobs`
3. 使用 FastAPI `BackgroundTasks` 异步执行 `run_backtest_task()`
4. 任务函数中：
   - 更新 job 状态为 `running`
   - 调用 `vnpy_ctabacktester` 执行回测
   - 解析回测结果 (权益曲线、交易记录、指标)
   - 保存到 `backtest_history` 表
   - 更新 job 状态为 `completed` 或 `failed`
5. 前端轮询 `GET /api/backtest/{job_id}` 获取进度
6. 完成后，前端可以查看结果图表和统计数据

### 4.4 批量回测流程

```
前端提交批量请求
    ↓
创建 BatchBacktestJob (batch_id)
    ↓
循环为每个策略/参数组合创建单个 BacktestJob
    ↓
所有 job 入队到 RQ (batch 队列或普通队列)
    ↓
前端轮询 GET /api/queue/batch/{batch_id}
    ↓
Worker 并行执行多个回测 (max_concurrent=4)
    ↓
每个 job 完成后更新 batch 进度
    ↓
全部完成后，批量结果存入 bulk_backtests + bulk_results
```

### 4.5 数据同步流程

```
DataSync Daemon (定时调度)
    ↓
查询 sync_status 表，确定需要同步的标的
    ↓
调用 Tushare/AkShare API
    ↓
增量写入 tushare.[table] (使用 INSERT ... ON DUPLICATE KEY UPDATE)
    ↓
记录 sync_log 同步日志
    ↓
更新 sync_status.last_sync_end
    ↓
检测缺失交易日，提供回补功能
```

---

## 五、API 概览

### 5.1 基础信息

- **Base URL**: `http://localhost:8000` (开发环境)
- **API 前缀**: `/api`
- **自动文档**: 
  - Swagger UI: `http://localhost:8000/docs`
  - ReDoc: `http://localhost:8000/redoc`
- **健康检查**: `GET /health` (检查 MySQL + Redis 连接)

### 5.2 认证接口

| 方法 | 路径 | 描述 |
|------|------|------|
| `POST` | `/api/auth/login` | 用户登录 |
| `POST` | `/api/auth/refresh` | 刷新 access token |
| `POST` | `/api/auth/logout` | 登出 |
| `GET` | `/api/auth/me` | 获取当前用户信息 |

**Headers**: 需要认证的接口需添加
```
Authorization: Bearer <access_token>
```

### 5.3 策略接口

| 方法 | 路径 | 描述 |
|------|------|------|
| `GET` | `/api/strategies` | 列出用户所有策略 |
| `POST` | `/api/strategies` | 创建新策略 |
| `GET` | `/api/strategies/{id}` | 获取策略详情 |
| `PUT` | `/api/strategies/{id}` | 更新策略 |
| `DELETE` | `/api/strategies/{id}` | 删除策略 |
| `POST` | `/api/strategies/{id}/validate` | 验证策略代码语法 |
| `GET` | `/api/strategies/{id}/history` | 获取策略回测历史 |

### 5.4 回测接口

| 方法 | 路径 | 描述 |
|------|------|------|
| `POST` | `/api/backtest` | 提交单次回测 |
| `GET` | `/api/backtest/{job_id}` | 查询回测任务状态和结果 |
| `POST` | `/api/backtest/{job_id}/cancel` | 取消进行中的回测 |
| `POST` | `/api/backtest/batch` | 提交批量回测 |
| `GET` | `/api/backtest/batch/{batch_id}` | 查询批次状态 |
| `GET` | `/api/backtest/history` | 列出回测历史 |
| `GET` | `/api/backtest/history/{id}` | 获取历史详情 |
| `DELETE` | `/api/backtest/history/{id}` | 删除历史记录 |

### 5.5 数据接口

| 方法 | 路径 | 描述 |
|------|------|------|
| `GET` | `/api/data/sync/status` | 获取数据同步状态 |
| `POST` | `/api/data/sync/tushare` | 手动触发 Tushare 同步 |
| `POST` | `/api/data/sync/akshare` | 手动触发 AkShare 同步 |
| `GET` | `/api/data/market/symbols` | 获取证券代码列表 |
| `GET` | `/api/data/market/calendar` | 获取交易日历 |
| `GET` | `/api/data/market/data` | 获取市场数据 (K线) |

### 5.6 队列状态接口

| 方法 | 路径 | 描述 |
|------|------|------|
| `GET` | `/api/queue/workers` | 列出所有 Worker 及其状态 |
| `GET` | `/api/queue/queues` | 列出所有队列及任务数 |
| `GET` | `/api/queue/jobs` | 列出所有作业 (已完成/失败) |
| `GET` | `/api/queue/stats` | 队列统计信息 |

### 5.7 系统监控接口

| 方法 | 路径 | 描述 |
|------|------|------|
| `GET` | `/api/system/sync-status` | 数据同步状态摘要 |
| `GET` | `/api/system/version` | 各组件版本信息 |
| `GET` | `/api/system/logs` | 获取应用日志 (需权限) |
| `POST` | `/api/system/cleanup` | 清理旧数据 (管理员) |

### 5.8 策略代码接口

| 方法 | 路径 | 描述 |
|------|------|------|
| `POST` | `/api/strategy_code/validate` | 仅验证代码语法 (无需创建策略) |
| `GET` | `/api/strategy_code/templates` | 获取策略代码模板 |
| `POST` | `/api/strategy_code/test` | 在沙箱中测试策略 (实验性) |

---

## 六、部署拓扑

### 6.1 Docker Compose 网络

```yaml
networks:
  tradermate_network:
    driver: bridge
```

所有服务连接至同一 Docker 网络 `tradermate_network`，通过 **服务名** 进行 DNS 解析。

#### 服务依赖关系

```yaml
depends_on:
  api:
    - mysql (condition: service_healthy)
    - redis (condition: service_healthy)
  worker:
    - mysql
    - redis
  tradermate:
    - mysql
  portal:
    - api (隐式，通过网络连接)
```

### 6.2 服务端口映射

| 服务 | 容器端口 | 宿主机端口 | 说明 |
|------|---------|-----------|------|
| `api` | 8000 | 8000 | FastAPI 服务 |
| `redis` | 6379 | 6379 | Redis (仅容器间访问，可省略映射) |
| `mysql` | 3306 | 3306 | MySQL (生产环境建议不映射) |
| `portal` | 80 | 5173 | Nginx 静态文件服务器 |
| `tradermate` | - | - | VN.PY 桌面应用 (非 Docker 部署) |

### 6.3 数据持久化

```yaml
volumes:
  mysql_data:      # MySQL 数据目录 (/var/lib/mysql)
  redis_data:      # Redis 持久化数据 (/data)
  ./logs:/app/logs # API 和 Worker 日志目录
```

### 6.4 环境变量 (.env)

**必需变量**:

```bash
# MySQL
MYSQL_PASSWORD=your_secure_password

# JWT
SECRET_KEY=your_jwt_secret_key

# Tushare (可选，用于数据同步)
TUSHARE_TOKEN=your_tushare_token
```

**可选变量**:

```bash
# 应用配置
DEBUG=true
APP_VERSION=1.0.0

# MySQL 连接 (默认值见 docker-compose.yml)
MYSQL_HOST=mysql
MYSQL_PORT=3306
MYSQL_USER=root

# Redis
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0

# CORS 允许的源 (JSON 数组格式)
CORS_ORIGINS=["http://localhost:5173","http://localhost:3000"]

# 并发控制
MAX_CONCURRENT_BACKTESTS=4
BACKTEST_TIMEOUT_SECONDS=600
```

### 6.5 生产环境配置建议

1. **网络隔离**: 使用自定义 Docker 网络，仅暴露必要端口 (80/443)
2. **数据库安全**: MySQL 不映射到宿主机，仅容器内访问
3. **密钥管理**: 使用 Docker Secrets 或外部配置中心 (如 HashiCorp Vault)
4. **日志收集**: 挂载卷到集中式日志系统 (ELK/Loki)
5. **备份**: 定期备份 MySQL 卷 `mysql_data`
6. **Worker 扩容**: 根据负载增加 `worker` 服务副本数
7. **HTTPS**: 前端使用反向代理 (Nginx/Traefik) 配置 SSL 证书

---

## 七、技术决策记录 (ADR)

### ADR-001: 选择 FastAPI 作为后端框架

**背景**: 需要快速构建高性能 REST API，自动生成文档。

**决策**: 使用 FastAPI (基于 Starlette + Pydantic)

**理由**:
- 异步支持，适合 I/O 密集型场景
- 自动 OpenAPI 文档，减少文档编写成本
- Pydantic 数据验证，类型安全
- 成熟生态，社区活跃

**后果**:
- 需要 Python 3.7+
- 团队成员需要学习 async/await 模式

---

### ADR-002: 使用 VN.PY 作为量化引擎

**背景**: 需要成熟的回测和执行框架，支持多市场。

**决策**: 集成 VN.PY (veighna)

**理由**:
- 国内最活跃的开源量化框架
- 支持股票、期货、期权等多品种
- 提供历史回测、实时交易、策略模板
- 已有良好的社区和文档

**后果**:
- 后端需兼容 VN.PY 的事件驱动架构
- 策略代码必须继承 `CtaTemplate`
- GUI 组件依赖 PySide6 (仅桌面端)

---

### ADR-003: 分离前后端，API 优先

**背景**: 需要同时支持 Web 端和桌面端，未来可能支持移动端。

**决策**: 前后端完全分离，API 作为唯一数据入口

**理由**:
- 客户端无关性，各端独立演进
- API 可以独立部署和水平扩展
- 便于第三方集成 (未来开放 API)

**后果**:
- 需要跨域支持 (CORS)
- 前端需自行处理状态管理
- API 版本管理需提前规划 (当前 v1)

---

### ADR-004: 使用 Redis RQ 而非 Celery

**背景**: 需要异步任务队列处理回测任务，避免阻塞 API。

**决策**: 选择 Redis Queue (RQ)

**理由**:
- API 已使用 Redis 作为缓存，减少依赖
- RQ 简单易用，适合中小规模任务
- 与 Python 生态集成良好
- 无需额外消息代理 (如 RabbitMQ)

**后果**:
- 任务监控需额外开发 (RQ 无内置 dashboard)
- 大规模分布式任务时可能需迁移至 Celery

---

### ADR-005: 容器化部署

**背景**: 需要简化开发、测试和生产部署流程。

**决策**: 使用 Docker Compose 定义多服务架构

**理由**:
- 一次构建，随处运行
- 环境一致，避免 "works on my machine" 问题
- 易于水平扩展
- 与 CI/CD 流程集成良好

**后果**:
- 需要团队成员熟悉 Docker
- Windows/macOS 下文件性能需注意 (卷挂载)
- 开发环境下需确保端口不冲突

---

## 八、安全考虑

### 8.1 认证与授权

- 使用 **JWT (JSON Web Token)** 进行无状态认证
- Access Token 有效期 30 分钟，Refresh Token 7 天
- 密码使用 **bcrypt** 哈希存储，成本因子 12
- API 接口通过 `get_current_user` 依赖注入获取用户

### 8.2 数据保护

- 数据库密码从环境变量读取，不提交到版本控制
- SQL 查询使用参数化，防止 SQL 注入
- 用户数据按 `user_id` 隔离，API 层强制校验所有权

### 8.3 输入验证

- 所有 API 请求体使用 Pydantic 模型验证
- 策略代码上传前进行 AST 语法检查和类继承验证
- 文件大小限制 (策略代码 < 1MB)

### 8.4 速率限制

- 当前未实现，建议在生产环境添加：
  - 登录接口: 5 次/分钟
  - 回测提交: 10 次/小时 (防止滥用资源)

---

## 九、性能与扩展性

### 9.1 性能瓶颈

| 组件 | 潜在瓶颈 | 缓解措施 |
|------|---------|---------|
| 回测引擎 | VN.PY 单进程计算 | Worker 水平扩展，并发控制 |
| 数据库 | 大量回测结果写入 | 分表分库，异步写入 |
| API | 高频状态轮询 | 改用 WebSocket 推送 |
| 前端 | 大量图表渲染 | 虚拟滚动，数据聚合 |

### 9.2 扩展性设计

- **Worker 无状态**: 可随时增加 `worker` 服务副本
- **数据库连接池**: SQLAlchemy 配置 `pool_size=20`, `max_overflow=10`
- **缓存热点数据**: Redis 缓存用户策略列表、回测结果摘要
- **分页**: 所有列表接口支持 `limit/offset` 或 `cursor`

---

## 十、未来演进路线

### Phase 2 (Q2 2026)

- [ ] WebSocket 实时推送回测进度
- [ ] 多策略组合回测 (Portfolio Backtest)
- [ ] 参数优化接口 (遗传算法/网格搜索)
- [ ] 策略市场 (社区共享策略)
- [ ] 实时交易 (对接券商 API)

### Phase 3 (H2 2026)

- [ ] 支持更多数据源 (Wind, JoinQuant, RiceQuant)
- [ ] 策略回测结果对比 (Benchmarking)
- [ ] 风险指标预警 (VaR, Stress Test)
- [ ] 云原生部署 (K8s Helm Chart)
- [ ] API 限流与计费系统

---

## 附录

### A. 配置文件参考

- `tradermate/.env.template` - 环境变量模板
- `tradermate/docker-compose.yml` - 开发环境编排
- `tradermate-portal/.env.example` - 前端环境变量
- `tradermate-portal/vite.config.ts` - Vite 配置

### B. 数据库初始化 SQL

```sql
-- tradermate 数据库初始化
CREATE DATABASE IF NOT EXISTS tradermate DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE tradermate;

-- 用户表
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- 策略表
CREATE TABLE strategies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    class_name VARCHAR(255) NOT NULL,
    description TEXT,
    parameters JSON,
    code LONGTEXT NOT NULL,
    version INT DEFAULT 1,
    is_active BOOLEAN DEFAULT TRUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    INDEX idx_user_id (user_id),
    INDEX idx_updated_at (updated_at DESC)
);

-- 回测历史表
CREATE TABLE backtest_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    strategy_id INT NOT NULL,
    job_id VARCHAR(36) NOT NULL,
    status ENUM('pending', 'running', 'completed', 'failed') DEFAULT 'pending',
    parameters JSON,
    result_summary JSON,
    result_data LONGTEXT,
    started_at DATETIME,
    completed_at DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (strategy_id) REFERENCES strategies(id) ON DELETE CASCADE,
    INDEX idx_user_job (user_id, job_id),
    INDEX idx_created_at (created_at DESC)
);

-- 批量回测表
CREATE TABLE bulk_backtests (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    batch_id VARCHAR(36) NOT NULL,
    total_jobs INT DEFAULT 0,
    completed_jobs INT DEFAULT 0,
    failed_jobs INT DEFAULT 0,
    status ENUM('pending', 'running', 'completed', 'failed') DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    completed_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    INDEX idx_batch_id (batch_id),
    INDEX idx_user_created (user_id, created_at DESC)
);

-- 批量结果表
CREATE TABLE bulk_results (
    id INT AUTO_INCREMENT PRIMARY KEY,
    bulk_backtest_id INT NOT NULL,
    strategy_id INT NOT NULL,
    job_id VARCHAR(36) NOT NULL,
    status ENUM('pending', 'running', 'completed', 'failed') DEFAULT 'pending',
    result_summary JSON,
    error_message TEXT,
    started_at DATETIME,
    completed_at DATETIME,
    FOREIGN KEY (bulk_backtest_id) REFERENCES bulk_backtests(id) ON DELETE CASCADE,
    FOREIGN KEY (strategy_id) REFERENCES strategies(id) ON DELETE CASCADE,
    INDEX idx_bulk_job (bulk_backtest_id, job_id)
);

-- 数据同步日志表
CREATE TABLE sync_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    exchange VARCHAR(20),
    interval VARCHAR(10),
    start_date DATE,
    end_date DATE,
    rows_inserted INT DEFAULT 0,
    status ENUM('success', 'failed', 'partial') DEFAULT 'success',
    error_message TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_source_symbol (source, symbol, exchange, interval),
    INDEX idx_created_at (created_at DESC)
);

-- 同步状态表
CREATE TABLE sync_status (
    id INT AUTO_INCREMENT PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    exchange VARCHAR(20),
    interval VARCHAR(10),
    last_sync_end DATE,
    next_sync_start DATE,
    is_active BOOLEAN DEFAULT TRUE,
    UNIQUE KEY uniq_source_symbol (source, symbol, exchange, interval)
);
```

### C. 常用命令

```bash
# 项目根目录
cd projects/TraderMate

# 启动所有服务 (生产)
docker-compose -f tradermate/docker-compose.yml up -d

# 查看日志
docker-compose -f tradermate/docker-compose.yml logs -f api
docker-compose -f tradermate/docker-compose.yml logs -f worker

# 停止服务
docker-compose -f tradermate/docker-compose.yml down

# 进入容器
docker exec -it tradermate_api bash

# 数据库备份
docker exec tradermate_mysql mysqldump -uroot -p"${MYSQL_PASSWORD}" tradermate > backup.sql

# 运行前端测试
cd tradermate-portal
npm run test

# 后端单元测试 (需进入容器)
docker exec tradermate_api pytest /app/tests/
```

---

**文档结束**

如需更新或补充，请在 GitHub Issues 中提交，或直接发起 Pull Request。
