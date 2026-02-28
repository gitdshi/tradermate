# TraderMate 开发者上手指南

**版本**: 1.0.0  
**最后更新**: 2026-02-28  
**目标读者**: 首次参与 TraderMate 项目的开发者

---

## 一、快速开始

本指南将帮助你在 **30 分钟** 内在本地搭建完整的 TraderMate 开发环境并运行第一个回测。

### 1.1 前置要求

| 工具 | 版本要求 | 安装指引 |
|------|---------|---------|
| **Python** | 3.11 或 3.12 (不支持 3.13) | [python.org](https://www.python.org/downloads/) |
| **Node.js** | 20+ LTS | [nodejs.org](https://nodejs.org/) |
| **Docker Desktop** | 最新稳定版 | [docker.com](https://www.docker.com/products/docker-desktop/) |
| **Docker Compose** | 随 Docker Desktop 安装 | 无需单独安装 |
| **Git** | 2.0+ | [git-scm.com](https://git-scm.com/) |
| **MySQL Client** (可选) | 任意 | 用于直接连接数据库 |

**验证安装**:

```bash
python3.11 --version  # 或 python3.12
node --version
docker --version
docker compose version
git --version
```

### 1.2 克隆项目

```bash
# 主仓库 (包含所有子项目)
cd ~/projects  # 或你的工作区
git clone https://github.com/tradermate/tradermate.git
cd tradermate
```

**项目结构预览**:

```
tradermate/
├── tradermate/          # 后端 API (FastAPI)
├── tradermate-portal/  # 前端门户 (React)
├── docs/               # 项目文档
├── skills/             # OpenClaw Agents
└── docker-compose.yml  # 开发环境编排 (在 tradermate/ 目录)
```

### 1.3 启动基础设施

建议使用 Docker Compose 一键启动 MySQL 和 Redis:

```bash
cd tradermate/tradermate
docker compose up -d mysql redis
```

**等待服务就绪**:

```bash
# 检查 MySQL
docker exec tradermate_mysql mysqladmin ping -h localhost -uroot -p"${MYSATE_PASSWORD}"  # 设置环境变量

# 检查 Redis
docker exec tradermate_redis redis-cli ping
# 应返回: PONG
```

### 1.4 配置环境变量

**复制模板**:

```bash
cd tradermate/tradermate
cp .env.template .env
```

**编辑 `.env`**:

```bash
# 必需: 设置数据库密码 (至少 16 位，含大小写字母和数字)
MYSQL_PASSWORD=YourSecurePassword123!

# 必需: JWT 密钥 (使用 openssl 生成)
SECRET_KEY=$(openssl rand -hex 32)

# 可选: Tushare token (用于数据同步)
# 申请地址: https://tushare.pro/register
TUSHARE_TOKEN=your_tushare_token_here

# 可选: 调试模式
DEBUG=true
```

### 1.5 初始化数据库

```bash
cd tradermate/tradermate

# 创建虚拟环境
python3.11 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 安装依赖
pip install --upgrade pip
pip install -r requirements.txt

# 运行数据库迁移 (Alembic)
alembic upgrade head
```

> **注意**: 如果 `alembic` 命令未找到，请检查 `requirements.txt` 是否包含 `alembic`，或手动安装: `pip install alembic`

**验证数据库**:

```bash
# 连接 MySQL 查看库列表
mysql -h127.0.0.1 -P3306 -uroot -p"${MYSQL_PASSWORD}" -e "SHOW DATABASES;"
# 应看到: information_schema, mysql, performance_schema, tushare, tradermate, vnpy
```

### 1.6 启动后端 API

```bash
# 继续在 tradermate/tradermate 目录 (虚拟环境已激活)

# 开发模式 (热重载)
uvicorn app.api.main:app --host 0.0.0.0 --port 8000 --reload

# 或使用脚本
bash scripts/api_service.sh start
```

**验证**:

```bash
curl http://localhost:8000/health
# 预期输出: {"status":"healthy", "dependencies":{"mysql":{"status":"healthy"},"redis":{"status":"healthy"}}}
```

访问自动文档: [http://localhost:8000/docs](http://localhost:8000/docs)

### 1.7 启动数据同步服务

```bash
# 新开一个终端，进入同一目录

# 启动数据同步 Daemon (后台运行)
python -m app.datasync.service.data_sync_daemon

# 或使用脚本
bash scripts/datasync_service.sh start
```

**验证**:

```bash
curl http://localhost:8000/api/data/sync/status
# 返回同步状态信息
```

### 1.8 启动后台 Worker

```bash
# 新开终端
cd tradermate/tradermate

# 启动 RQ Worker
rq worker --url redis://localhost:6379/0 backtest optimization

# 或使用脚本
bash scripts/worker_service.sh start
```

**验证**:

```bash
curl http://localhost:8000/api/queue/workers
# 应看到 worker 在线
```

### 1.9 启动前端门户

```bash
# 新开终端
cd tradermate-portal

# 安装依赖 (首次)
npm install

# 开发模式
npm run dev

# 或使用脚本
bash scripts/portal_service.sh start
```

前端将在 [http://localhost:5173](http://localhost:5173) 启动。

### 1.10 注册首个用户

**方式一: 使用 API 直接创建** (推荐开发者)

```bash
curl -X POST "http://localhost:8000/api/auth/register" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "developer",
    "email": "dev@tradermate.local",
    "password": "YourPassword123!"
  }'
```

如果注册接口未开放，使用 SQL 直接插入:

```bash
mysql -h127.0.0.1 -P3306 -uroot -p"${MYSQL_PASSWORD}" tradermate -e "
INSERT INTO users (username, email, hashed_password, is_active)
VALUES ('developer', 'dev@tradermate.local', '\$2b\$12\$YOUR_BCRYPT_HASH', 1);
"
```

> 生成 bcrypt 哈希: `python -c "import bcrypt; print(bcrypt.hashpw(b'YourPassword123!', bcrypt.gensalt()).decode())"`

**方式二: 前端界面注册** (如果已实现注册页面)

1. 访问 [http://localhost:5173](http://localhost:5173)
2. 点击 "Register" → 填写表单 → 提交

### 1.11 运行第一个回测

**步骤 1: 创建策略**

```bash
# 登录获取 token
TOKEN=$(curl -X POST "http://localhost:8000/api/auth/login" \
  -H "Content-Type: application/json" \
  -d '{"username":"developer","password":"YourPassword123!"}' \
  | jq -r '.access_token')

# 创建简单 Moving Average 策略
curl -X POST "http://localhost:8000/api/strategies" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Test MA Cross",
    "class_name": "MaCrossStrategy",
    "description": "Simple MA crossover test",
    "code": "from vnpy_ctastrategy import CtaTemplate, BarData\n\nclass MaCrossStrategy(CtaTemplate):\n    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):\n        super().__init__(cta_engine, strategy_name, vt_symbol, setting)\n        self.bar = None\n    def on_init(self):\n        self.write_log(\"Strategy initialized\")\n    def on_bar(self, bar: BarData):\n        self.bar = bar\n        self.write_log(f\"Bar: {bar.datetime} close={bar.close_price}\")",
    "parameters": {"fast_window": 10, "slow_window": 20}
  }'
```

**步骤 2: 提交回测**

```bash
curl -X POST "http://localhost:8000/api/backtest" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "strategy_id": 1,
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "initial_capital": 100000,
    "frequency": "1d",
    "symbol": "000001.SZ",
    "exchange": "SZSE"
  }'
```

**步骤 3: 查询结果**

```bash
# 提取 job_id
JOB_ID="your-job-id-here"

# 轮询状态
while true; do
  STATUS=$(curl -s -H "Authorization: Bearer $TOKEN" \
    "http://localhost:8000/api/backtest/$JOB_ID" | jq -r '.status')
  echo "Status: $STATUS"
  [[ "$STATUS" == "completed" || "$STATUS" == "failed" ]] && break
  sleep 3
done

# 获取结果
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/backtest/$JOB_ID" | jq .
```

---

## 二、开发工作流

### 2.1 Git 分支策略

我们采用 **GitHub Flow** (简化版):

```
main          ← 生产分支 (稳定，已发布)
develop       ← 开发分支 (集成测试)
feature/xxx   ← 功能分支 (从 develop 创建)
hotfix/xxx    ← 热修复分支 (从 main 创建)
```

**标准流程**:

1. 从 `develop` 拉取新分支: `git checkout -b feature/add-backtest-optimization develop`
2. 开发并提交: `git commit -m "feat: add batch optimization support"`
3. 推送分支: `git push -u origin feature/add-backtest-optimization`
4. 创建 Pull Request 到 `develop`
5. 通过 CI 和 Code Review 后合并
6. `develop` 测试通过后合并到 `main` 并发布

**提交信息规范** (Conventional Commits):

```
feat: 新功能
fix: bug 修复
docs: 文档变更
style: 代码格式调整 (不影响逻辑)
refactor: 代码重构
test: 测试相关
chore: 构建/工具变更
```

示例: `feat: support batch backtest with parameter sweep`

### 2.2 代码质量

#### 后端 (Python)

- **格式化**: `black .` (行宽 88)
- **导入排序**: `isort .`
- **Lint**: `ruff .` 或 `flake8`
- **类型检查**: `mypy app/`

**预提交钩子** (推荐):

```bash
pip install pre-commit
pre-commit install
```

项目包含 `.pre-commit-config.yaml`，自动运行 `black`, `isort`, `ruff`。

#### 前端 (TypeScript/React)

- **格式化**: `npm run format` (使用 Prettier)
- **Lint**: `npm run lint`
- **类型检查**: `tsc --noEmit`
- **测试**: `npm run test`

### 2.3 测试

#### 后端单元测试

```bash
cd tradermate/tradermate
pytest tests/ -v --cov=app --cov-report=html
```

**添加新测试**:

- 位置: `tests/unit/` 或 `tests/integration/`
- 文件名: `test_<module>.py`
- 使用 `pytest` fixtures 管理数据库连接

**运行特定测试**:

```bash
pytest tests/unit/test_strategy_service.py::test_create_strategy -v
```

#### 前端单元测试

```bash
cd tradermate-portal
npm run test:run  # 单次运行
npm run test      # 监听模式
```

#### E2E 测试 (Playwright)

```bash
cd tradermate-portal
npm run test:e2e  # 无头模式
npm run test:e2e:headed  # 显示浏览器
```

**CI 流程**: GitHub Actions 自动运行所有测试，覆盖率要求:
- 后端: >= 80%
- 前端: >= 70%

### 2.4 调试技巧

#### 后端调试

1. **使用 VSCode 调试** (推荐):

   打开 `tradermate/tradermate/.vscode/launch.json`:

   ```json
   {
     "version": "0.2.0",
     "configurations": [
       {
         "name": "Debug API",
         "type": "debugpy",
         "request": "launch",
         "module": "uvicorn",
         "args": ["app.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"],
         "jinja": true,
         "envFile": "${workspaceFolder}/.env"
       }
     ]
   }
   ```

   按 `F5` 启动调试，在 `app/api/main.py` 或任意地方设置断点。

2. **日志查看**:

   ```bash
   # Docker 容器日志
   docker logs -f tradermate_api

   # 或实时查看日志文件
   tail -f logs/api.log
   ```

3. **数据库调试**:

   ```bash
   # 连接数据库
   mysql -h127.0.0.1 -P3306 -uroot -p"${MYSQL_PASSWORD}" tradermate

   # 查看当前连接
   SHOW PROCESSLIST;
   ```

#### 前端调试

1. **VSCode + Chrome Debug**:

   安装 `Debugger for Chrome` 扩展，配置 `launch.json`:

   ```json
   {
     "type": "chrome",
     "request": "launch",
     "name": "React: Vite",
     "url": "http://localhost:5173",
     "webRoot": "${workspaceFolder}/src"
   }
   ```

2. **React DevTools**: 安装浏览器扩展，检查组件状态
3. **Network 面板**: 查看 API 请求/响应
4. **Console**: 检查前端错误

#### 常见调试场景

**API 返回 500 错误**:

```bash
# 查看 API 日志
docker exec tradermate_api tail -f /app/logs/api.log
# 或
docker logs -f tradermate_api
```

**数据库连接失败**:

- 确认 MySQL 容器运行: `docker ps | grep mysql`
- 检查 `.env` 中 `MYSQL_PASSWORD` 是否与 `docker-compose.yml` 中的一致
- 检查网络: `docker network inspect tradermate_network`

**前端请求 CORS 错误**:

- 检查 `CORS_ORIGINS` 配置
- 确认 API 已启用 CORS 中间件
- 开发环境下，前端代理到 API (见下)

### 2.5 开发工具配置

#### 前端代理 (避免 CORS)

在 `tradermate-portal/vite.config.ts` 中配置:

```typescript
export default defineConfig({
  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, '/api')
      }
    }
  }
})
```

这样前端 `fetch('/api/...')` 会自动代理到后端。

---

## 三、项目结构详解

### 3.1 后端结构 (tradermate/)

```
tradermate/
├── app/
│   ├── api/
│   │   ├── main.py              # FastAPI 应用入口
│   │   ├── routes/              # 路由层
│   │   │   ├── auth.py
│   │   │   ├── strategies.py
│   │   │   ├── backtest.py
│   │   │   ├── data.py
│   │   │   ├── queue.py
│   │   │   ├── system.py
│   │   │   └── strategy_code.py
│   │   ├── models/              # Pydantic 模型
│   │   │   ├── user.py
│   │   │   ├── strategy.py
│   │   │   ├── backtest.py
│   │   │   └── ...
│   │   └── services/            # 应用服务层
│   │       ├── auth_service.py
│   │       ├── strategy_service.py
│   │       └── ...
│   ├── domains/                 # 业务域 (DDD)
│   │   ├── auth/
│   │   ├── strategies/
│   │   ├── backtests/
│   │   ├── extdata/
│   │   ├── market/
│   │   └── jobs/
│   ├── infrastructure/          # 基础设施
│   │   ├── config/config.py
│   │   ├── db/connections.py
│   │   └── logging/
│   ├── datasync/                # 数据同步
│   │   ├── service/
│   │   │   ├── data_sync_daemon.py
│   │   │   ├── tushare_ingest.py
│   │   │   └── ...
│   │   └── tasks/
│   ├── worker/
│   │   └── service/
│   │       └── tasks.py         # RQ 任务定义
│   ├── main.py                  # VN.PY 桌面应用入口 (旧版)
│   └── strategies/              # 内置策略示例
│       ├── turtle_trading.py
│       └── triple_ma_strategy.py
├── scripts/                     # 服务启动脚本
│   ├── api_service.sh
│   ├── worker_service.sh
│   ├── datasync_service.sh
│   └── ...
├── tests/
│   ├── unit/
│   ├── integration/
│   └── conftest.py
├── alembic/                     # 数据库迁移
│   ├── versions/
│   └── env.py
├── docker-compose.debug.yml     # 调试环境编排
├── Dockerfile.api               # API 镜像构建
├── requirements.txt
├── .env.template
└── README.md
```

---

### 3.2 前端结构 (tradermate-portal/)

```
tradermate-portal/
├── src/
│   ├── App.tsx                  # 根组件
│   ├── main.tsx                 # 应用入口
│   ├── index.css                # 全局样式
│   ├── components/              # 可复用 UI 组件
│   │   ├── StrategyList.tsx
│   │   ├── BacktestForm.tsx
│   │   ├── EquityCurveChart.tsx
│   │   ├── TradingChart.tsx
│   │   ├── RiskMetrics.tsx
│   │   ├── SymbolSearch.tsx
│   │   └── ...
│   ├── pages/                   # 页面级组件
│   │   ├── Login.tsx
│   │   ├── Dashboard.tsx
│   │   ├── Strategies.tsx
│   │   ├── Backtest.tsx
│   │   ├── BatchBacktest.tsx
│   │   ├── Results.tsx
│   │   ├── Data.tsx
│   │   ├── System.tsx
│   │   └── ...
│   ├── stores/                  # Zustand 状态存储
│   │   ├── auth.store.ts
│   │   ├── strategy.store.ts
│   │   ├── backtest.store.ts
│   │   └── ...
│   ├── lib/                     # 第三方库封装
│   │   ├── api.ts               # API 客户端
│   │   ├── auth.ts              # 认证工具
│   │   └── formatters.ts        # 数据格式化
│   ├── types/                   # TypeScript 类型定义
│   │   ├── index.ts
│   │   ├── api.ts
│   │   ├── strategy.ts
│   │   └── backtest.ts
│   └── test/                    # 测试文件
│       ├── setup.ts
│       ├── utils.tsx
│       ├── mockData.ts
│       ├── api.test.ts
│       └── ...
├── public/
├── Dockerfile                   # 生产构建镜像 (Nginx)
├── nginx.conf                   # Nginx 配置
├── vite.config.ts
├── tailwind.config.js
├── tsconfig.json
├── package.json
└── README.md
```

---

## 四、常见问题与解决方案 (FAQ)

### Q1: `ModuleNotFoundError: No module named 'app'`

**原因**: Python 路径未正确设置。

**解决**:

```bash
# 确认在 tradermate/tradermate 目录 (项目根)
cd /path/to/tradermate/tradermate

# 检查 sys.path 包含项目根
python -c "import sys; print(sys.path)"
# 应能看到 '.' 或项目绝对路径
```

如果使用 VSCode 调试，确保 `launch.json` 中 `cwd` 设置为 `${workspaceFolder}`。

---

### Q2: MySQL 连接被拒绝 (Connection refused)

**原因**: MySQL 容器未运行或网络未就绪。

**解决**:

```bash
# 1. 检查容器状态
docker ps | grep mysql

# 2. 如果未运行，启动
docker compose up -d mysql

# 3. 等待健康检查通过 (约 30 秒)
docker inspect tradermate_mysql | grep -A 10 Health

# 4. 测试连接
mysql -hmysql.host.docker.internal -P3306 -uroot -p"${MYSQL_PASSWORD}"
# 或使用 docker 内部网络
docker exec tradermate_api mysql -h mysql -uroot -p"${MYSQL_PASSWORD}" -e "SELECT 1"
```

---

### Q3: 数据同步失败: `Tushare token invalid`

**原因**: `.env` 中 `TUSHARE_TOKEN` 无效或过期。

**解决**:

1. 访问 [Tushare](https://tushare.pro/) 申请新 token
2. 更新 `.env`: `TUSHARE_TOKEN=your_new_token`
3. 重启 API 和 DataSync 服务

**临时绕过** (仅开发测试):

```python
# 在代码中禁用数据同步检查 (不推荐)
# app/domains/extdata/service.py
# 注释掉 tushare 初始化逻辑
```

---

### Q4: Worker 不处理任务 (Job stuck in 'queued')

**原因**:
- Redis 连接失败
- Worker 未启动或崩溃
- 队列名称不匹配

**解决**:

```bash
# 1. 检查 Redis
docker exec tradermate_redis redis-cli ping

# 2. 检查 Worker 进程
docker ps | grep worker
# 或查看日志
docker logs tradermate_worker

# 3. 验证队列名称匹配
# API 中使用: rq.Queue('backtest', connection=redis_conn)
# Worker 启动: rq worker --url redis://... backtest
# 确保队列名 'backtest' 一致
```

**重启 Worker**:

```bash
docker compose restart worker
# 或手动停止并启动
docker stop tradermate_worker
docker rm tradermate_worker
docker compose up -d worker
```

---

### Q5: 回测任务超时 (600s)

**原因**: 复杂策略或长周期回测耗时超过 `backtest_timeout_seconds`。

**解决**:

1. **开发调试**: 缩短回测周期或标的数量
2. **增加超时**: 在 `.env` 中设置 `BACKTEST_TIMEOUT_SECONDS=1800`
3. **异步监控**: 使用 `/api/backtest/{job_id}` 轮询，避免 HTTP 超时

---

### Q6: 前端编译错误: `TypeError: Cannot find module '...'`

**原因**: `node_modules` 缺失或损坏。

**解决**:

```bash
cd tradermate-portal
rm -rf node_modules package-lock.json
npm install
```

如果问题持续，清除 npm 缓存:

```bash
npm cache clean --force
npm install
```

---

### Q7: Docker 卷挂载性能慢 (macOS/Windows)

**现象**: 文件修改后热重载慢，Docker 日志读写卡顿。

**缓解措施**:

1. **使用 cached/consistent 模式** (docker-compose):

   ```yaml
   volumes:
     - ./app:/app/app:cached  # macOS/Windows
   ```

2. **排除 node_modules 挂载** (前端):

   ```yaml
   # 只挂载源码，不挂载依赖
   - ./src:/app/src
   - ./public:/app/public
   ```

3. **升级 Docker Desktop**: 确保使用最新版本，优化文件共享

---

### Q8: Alembic 迁移失败: `Can't locate revision identified by '...'`

**原因**: 迁移历史与数据库不匹配。

**解决**:

```bash
# 1. 查看当前迁移头
alembic current

# 2. 如果无头或错误，重新标记
alembic stamp head

# 3. 或重新生成迁移脚本
alembic revision --autogenerate -m "Initial"
alembic upgrade head
```

**生产警告**: 不要在生产环境使用 `--autogenerate` 前手动修改数据库。

---

### Q9: 策略代码验证失败: `SyntaxError: invalid syntax`

**原因**: 提交的策略代码存在 Python 语法错误。

**解决**:

1. **本地验证**:

   ```bash
   python -m py_compile your_strategy.py
   ```

2. **检查缩进**: 策略代码必须使用 **4 空格** 缩进，不能混用 Tab
3. **检查类定义**: 必须继承 `CtaTemplate` 并实现必要方法

   ```python
   from vnpy_ctastrategy import CtaTemplate, BarData

   class MyStrategy(CtaTemplate):
       def on_init(self):
           pass
       def on_bar(self, bar: BarData):
           pass
   ```

4. **导入限制**: 策略只能导入标准库和 vnpy 相关模块，禁止网络访问、文件系统操作

---

### Q10: 如何在生产环境部署?

**简要步骤**:

1. **准备服务器**:
   - Ubuntu 22.04 LTS
   - Docker + Docker Compose
   - 域名 + SSL 证书

2. **克隆代码**:

   ```bash
   git clone https://github.com/tradermate/tradermate.git
   cd tradermate/tradermate
   ```

3. **配置环境**:

   ```bash
   cp .env.template .env
   # 编辑 .env，设置生产配置:
   # DEBUG=false
   # MYSQL_PASSWORD=强密码
   # SECRET_KEY=随机字符串
   # CORS_ORIGINS=["https://yourdomain.com"]
   ```

4. **构建镜像** (可选，可拉取预构建镜像):

   ```bash
   docker compose build
   ```

5. **启动**:

   ```bash
   docker compose up -d
   ```

6. **配置 Nginx 反向代理** (见 `docs/deployment/nginx.conf`)

7. **备份策略**:
   - 数据库每日自动备份: `docker exec tradermate_mysql mysqldump ...`
   - 日志轮转: `logrotate`

详细生产部署指南见 `docs/deployment/PRODUCTION.md`。

---

## 五、API 使用示例

### 5.1 认证流程

```bash
# 1. 注册 (如果是新用户)
curl -X POST http://localhost:8000/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"alice","email":"alice@example.com","password":"SecurePass123!"}'

# 2. 登录
TOKEN=$(curl -s -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"alice","password":"SecurePass123!"}' | jq -r '.access_token')

# 3. 使用 token 访问受保护接口
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/api/strategies
```

### 5.2 CRUD 策略

```bash
# 创建策略
curl -X POST http://localhost:8000/api/strategies \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Dual MA",
    "class_name": "DualMaStrategy",
    "description": "Fast/Slow MA crossover",
    "code": "from vnpy_ctastrategy import CtaTemplate, BarData\n\nclass DualMaStrategy(CtaTemplate):\n    def on_init(self):\n        self.write_log(\"Init\")\n    def on_bar(self, bar: BarData):\n        pass',
    "parameters": {"fast": 10, "slow": 30}
  }'

# 列出策略
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/api/strategies | jq

# 获取单个
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/api/strategies/1 | jq

# 更新
curl -X PUT http://localhost:8000/api/strategies/1 \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"description":"Updated description"}'

# 删除
curl -X DELETE -H "Authorization: Bearer $TOKEN" http://localhost:8000/api/strategies/1
```

### 5.3 提交回测

```bash
# 单次回测
curl -X POST http://localhost:8000/api/backtest \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "strategy_id": 1,
    "start_date": "2023-01-01",
    "end_date": "2023-12-31",
    "initial_capital": 100000,
    "frequency": "1d",
    "symbol": "000001.SZ",
    "exchange": "SZSE"
  }' | jq .job_id  # 提取 job_id

# 查询状态
JOB_ID="your-job-id"
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/api/backtest/$JOB_ID | jq

# 批量回测 (参数扫描)
curl -X POST http://localhost:8000/api/backtest/batch \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "base": {
      "strategy_id": 1,
      "start_date": "2023-01-01",
      "end_date": "2023-12-31",
      "initial_capital": 100000,
      "symbol": "000001.SZ",
      "exchange": "SZSE"
    },
    "parameter_grid": {
      "fast_window": [5, 10, 15],
      "slow_window": [20, 30, 40]
    }
  }'
```

### 5.4 数据查询

```bash
# 获取市场数据 (日线)
curl "http://localhost:8000/api/data/market/data?symbol=000001.SZ&exchange=SZSE&start=2024-01-01&end=2024-01-31&frequency=1d" \
  -H "Authorization: Bearer $TOKEN" | jq

# 获取交易日历
curl http://localhost:8000/api/data/market/calendar?exchange=SSE&start=2024-01-01&end=2024-12-31 \
  -H "Authorization: Bearer $TOKEN" | jq
```

---

## 六、贡献指南

### 6.1 如何贡献

1. **Fork 本仓库**
2. **创建特性分支**: `git checkout -b feature/AmazingFeature`
3. **提交更改**: `git commit -m 'Add some AmazingFeature'`
4. **推送到分支**: `git push origin feature/AmazingFeature`
5. **开启 Pull Request** 到 `develop` 分支

### 6.2 Pull Request 要求

- [ ] 代码符合现有风格 (运行 `black`, `isort`, `ruff`)
- [ ] 新增测试覆盖核心逻辑 (后端 >= 80%, 前端 >= 70%)
- [ ] 更新文档 (如有 API 变更，同步更新 `docs/api/` 或本文档)
- [ ] PR 描述清晰，关联 Issue 编号

### 6.3 代码审查清单

**后端**:
- [ ] 是否有 SQL 注入风险？ (必须用参数化查询)
- [ ] 是否有权限校验？ (所有接口必须验证 `user_id`)
- [ ] 异常处理是否完善？ (返回适当的 HTTP 状态码)
- [ ] 是否有性能问题？ (N+1 查询, 未索引)

**前端**:
- [ ] 是否处理 loading/error 状态？
- [ ] 表单验证是否完整？
- [ ] 是否滥用 useEffect？ (避免无限循环)
- [ ] 组件是否可复用？ (遵循单一职责)

---

## 七、调试与监控

### 7.1 日志位置

| 服务 | 日志路径 (容器内) | 查看命令 |
|------|------------------|---------|
| API | `/app/logs/api.log` | `docker logs tradermate_api` |
| Worker | `/app/logs/worker.log` | `docker logs tradermate_worker` |
| DataSync | `/app/logs/datasync.log` | `docker logs tradermate_datasync` |
| MySQL | `docker logs tradermate_mysql` | |
| Redis | `docker logs tradermate_redis` | |

**日志级别配置**: `app/infrastructure/logging/logging_setup.py`

### 7.2 性能监控

**使用 `fastapi-profiler`** (可选):

```bash
pip install fastapi-profiler
# 在 app/api/main.py 添加
from fastapi_profiler import ProfilerMiddleware
app.add_middleware(ProfilerMiddleware)
```

访问 `/profile` 查看性能数据。

**Prometheus + Grafana** (生产环境):
- 集成 `prometheus-fastapi-instrumentator`
- 导出 metrics: `GET /metrics`
- 配置 Grafana dashboard 监控 QPS、延迟、错误率

### 7.3 数据库慢查询

```sql
-- 查看慢查询日志 (MySQL)
SHOW VARIABLES LIKE 'slow_query_log%';
-- 开启慢查询日志 (>= 2 秒)
SET GLOBAL slow_query_log = 'ON';
SET GLOBAL long_query_time = 2;

-- 分析慢查询
SELECT * FROM mysql.slow_log ORDER BY start_time DESC LIMIT 10;
```

**常见慢查询优化**:
- `backtest_history` 表按 `user_id + created_at` 建立复合索引
- `strategies` 表查询 `user_id` 需索引
- 大表 `tushare.stock_daily` 按 `ts_code + trade_date` 联合索引

---

## 八、扩展与自定义

### 8.1 添加新数据源

1. **创建 DAO**: `app/domains/extdata/dao/your_source_dao.py`
2. **实现同步服务**: `app/domains/extdata/service.py` 中添加方法
3. **注册路由**: `app/api/routes/data.py` 添加 `/data/sync/your_source`
4. **更新 `sync_status` 表**: 添加对应数据源的状态跟踪

### 8.2 添加新的策略类型

当前仅支持 CTA 策略 (`vnpy_ctastrategy`)。扩展支持:

1. **创建策略 App**: 参考 `vnpy_ctastrategy` 实现自己的 `MyStrategyApp`
2. **集成到 `main.py`**: 添加 `main_engine.add_app(MyStrategyApp)`
3. **API 适配**: 前端需要支持新策略类型的参数配置 UI

### 8.3 自定义回测指标

修改 `app/domains/backtests/service.py` 中的指标计算:

```python
def calculate_metrics(result: dict) -> dict:
    metrics = {
        "sharpe_ratio": ...,
        "max_drawdown": ...,
    }
    # 添加自定义指标
    metrics["win_rate"] = ...
    metrics["profit_factor"] = ...
    return metrics
```

---

## 九、求助与社区

### 9.1 内部资源

- **项目 Wiki**: [内部链接] - 详细设计文档
- **API 文档**: http://localhost:8000/docs (本地运行)
- **架构决策记录 (ADR)**: `docs/architecture/adr/`
- **问题追踪**: GitHub Issues (https://github.com/tradermate/tradermate/issues)

### 9.2 外部资源

- **VN.PY 官方文档**: https://www.vnpy.com/docs/
- **FastAPI 文档**: https://fastapi.tiangolo.com/
- **Tailwind CSS**: https://tailwindcss.com/docs
- **Zustand**: https://zustand-demo.pmnd.rs/

### 9.3 联系维护者

- **项目 maintainer**: Dan (PM)
- **后端负责人**: [待定]
- **前端负责人**: [待定]
- **Slack/Discord**: trapermate-team (邀请链接)

**紧急问题**: 直接在 GitHub Issues 标注 `P0` 并 @maintainer

---

## 十、附录

### A. 环境变量完整清单

```bash
# 应用配置
APP_NAME=TraderMate API
APP_VERSION=1.0.0
DEBUG=false

# 数据库
MYSQL_HOST=mysql
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=********
TRADERMATE_DB=tradermate
TUSHARE_DB=tushare

# Redis
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0

# JWT
SECRET_KEY=********
JWT_SECRET_KEY=********  # 同 SECRET_KEY
JWT_ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30
REFRESH_TOKEN_EXPIRE_DAYS=7

# 数据源
TUSHARE_TOKEN=********  # 可选

# CORS
CORS_ORIGINS=["http://localhost:5173","http://localhost:3000"]

# 回测限制
MAX_CONCURRENT_BACKTESTS=4
BACKTEST_TIMEOUT_SECONDS=600

# 日志
LOG_LEVEL=INFO
LOG_FILE=/app/logs/api.log
```

### B. 端口占用检查

```bash
# macOS/Linux
lsof -i :8000
lsof -i :5173
lsof -i :3306
lsof -i :6379

# Windows
netstat -ano | findstr :8000
```

如果端口被占用，停止占用进程或修改 `docker-compose.yml` 中的端口映射。

### C. 数据库连接池配置

修改 `app/infrastructure/db/connections.py`:

```python
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

engine = create_engine(
    url,
    poolclass=QueuePool,
    pool_size=20,          # 连接池大小
    max_overflow=10,       # 最大溢出连接
    pool_pre_ping=True,    # 连接前检查
    pool_recycle=3600,     # 1 小时回收连接
    echo=False             # 生产环境关闭 SQL 日志
)
```

### D. 常用 SQL 查询

```sql
-- 统计用户策略数量
SELECT user_id, COUNT(*) as strategy_count
FROM strategies
GROUP BY user_id
ORDER BY strategy_count DESC;

-- 查找最近 7 天活跃用户
SELECT DISTINCT user_id
FROM backtest_history
WHERE started_at >= DATE_SUB(NOW(), INTERVAL 7 DAY);

-- 回测任务失败分析
SELECT status, COUNT(*) as count
FROM backtest_history
GROUP BY status;

-- 数据同步缺失统计
SELECT source, COUNT(*) as missing_dates
FROM sync_status
WHERE last_sync_end IS NULL OR next_sync_start > CURDATE()
GROUP BY source;

-- 清理 90 天前的旧日志 (定期维护)
DELETE FROM sync_log WHERE created_at < DATE_SUB(NOW(), INTERVAL 90 DAY);
```

---

**祝开发愉快! 🚀**

如有疑问，请查阅项目 Wiki 或提 Issue。我们欢迎所有贡献!
