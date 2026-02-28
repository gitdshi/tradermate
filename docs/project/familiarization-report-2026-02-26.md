# TraderMate 项目熟悉报告

**日期**: 2026-02-26  
**作者**: Dan (PM)  
**目的**: 团队项目熟悉度概览

---

## 📦 仓库概览

### 1. tradermate (后端)

**技术栈**: 
- FastAPI (Python 3.x)
- vn.py 交易框架
- MySQL (Docker)
- Redis + RQ (后台任务)
- Docker + docker-compose

**目录结构**:
```
tradermate/
├── app/
│   ├── api/           # FastAPI 路由与服务层
│   ├── domains/       # 领域模型
│   │   ├── auth/      # 认证
│   │   ├── strategies/# 策略管理
│   │   ├── backtests/ # 回测
│   │   ├── market/    # 市场数据
│   │   ├── jobs/      # 任务队列
│   │   └── extdata/   # 外部数据 (Tushare)
│   ├── datasync/      # 数据同步服务
│   ├── worker/        # 后台工作进程
│   └── infrastructure/ # 基础设施 (配置、日志、DB)
├── docs/              # 文档
├── scripts/           # 服务管理脚本
└── mysql/             # MySQL 初始化
```

**已实现功能**:
- ✅ JWT 认证与用户管理
- ✅ 策略 CRUD (创建/读取/更新/删除)
- ✅ 单个与批量回测 (同步/异步队列)
- ✅ 市场数据访问 (股票列表、历史K线、指标)
- ✅ Tushare 数据自动同步
- ✅ 数据库设计 (用户、策略、回测结果、任务队列)
- ✅ Docker 容器化部署
- ✅ 服务生命周期管理脚本

**关键脚本**:
- `scripts/api_service.sh` - 启动/停止 API 服务
- `scripts/worker_service.sh` - 启动/停止 工作进程
- `scripts/datasync_service.sh` - 启动/停止 数据同步

### 2. tradermate-portal (前端)

**技术栈**:
- React 19 + TypeScript
- Vite (构建工具)
- Tailwind CSS (样式)
- Zustand (状态管理)
- TanStack React Query (数据获取)
- Recharts (图表)
- Monaco Editor (策略代码编辑器)
- Playwright + Vitest (测试)

**目录结构**:
```
tradermate-portal/
├── src/
│   ├── components/    # 可复用组件
│   │   ├── StrategyList.tsx
│   │   ├── BacktestForm.tsx
│   │   ├── BacktestJobList.tsx
│   │   ├── PortfolioManagement.tsx
│   │   ├── StrategyOptimization.tsx
│   │   ├── EquityCurveChart.tsx
│   │   ├── TradingChart.tsx
│   │   ├── RiskMetrics.tsx
│   │   ├── MarketOverview.tsx
│   │   ├── MarketDataView.tsx
│   │   └── ...
│   ├── stores/        # Zustand 状态 stores
│   ├── services/      # API 调用封装
│   ├── types/         # TypeScript 类型定义
│   ├── test/          # 测试文件
│   ├── App.tsx
│   └── main.tsx
├── e2e/               # Playwright E2E 测试
├── scripts/           # 前端服务脚本
└── public/
```

**已实现页面/功能**:
- ✅ 用户认证 (登录/注册)
- ✅ 策略列表与管理
- ✅ 回测配置与提交 (单个/批量)
- ✅ 回测结果查看 (收益曲线、指标)
- ✅ 投资组合管理
- ✅ 策略优化界面
- ✅ 市场概览与数据查看
- ✅ 内置策略库
- ✅ 策略代码编辑器 (Monaco)
- ✅ 比较多个回测结果
- ✅ 响应式布局

---

## 🎯 当前项目状态

根据 Git 提交历史 (截至 2026-02-12):

**后端**:
- 最近提交集中在数据同步 (datasync) 的优化与稳定性改进
- 完成了服务重组: 将业务逻辑移入 `service/` 子模块
- 基础设施统一到 `app/infrastructure/`
- 标准化了服务管理脚本

**前端**:
- 最近提交集中在 UI 改进:
  - 批量回测总结显示
  - 策略参数美化显示
  - 从文件加载策略功能
  - Dashboard 同步状态显示
- 添加了 portal_service.sh 管理脚本

**整体**: 项目架构清晰，代码组织良好，具备完整的前后端分离架构和 CI/CD 基础。

---

## 📋 团队熟悉建议

为了让团队快速上手，建议按以下顺序进行：

### Phase 1: 环境搭建与运行 (1天)
1. **后端**: 克隆仓库 → 配置 `.env` (Tushare token) → 启动 MySQL → 运行 API 服务
2. **前端**: 克隆仓库 → 安装依赖 → 启动 Vite dev server
3. **验证**: 登录前端 → 查看市场数据 → 创建简单策略 → 提交回测

### Phase 2: 代码探索 (2-3天)
**后端**:
- 阅读 `docs/API_README.md` 了解 API 结构
- 查看 `app/api/routes/` 了解端点定义
- 查看 `app/domains/` 理解数据模型
- 查看 `app/datasync/service/` 了解数据同步逻辑
- 查看 `app/worker/service/` 了解后台任务

**前端**:
- 运行项目，体验所有页面
- 查看 `src/App.tsx` 了解路由结构
- 查看 `src/stores/` 理解状态管理
- 查看 `src/services/` 了解 API 调用
- 查看关键组件 (StrategyList, BacktestForm, EquityCurveChart)

### Phase 3: 测试运行 (1天)
- 后端: 运行单元测试 (如有)
- 前端: `npm run test:run` + `npm run test:e2e`
- 了解测试覆盖率情况

### Phase 4: 团队同步会议 (1小时)
- 每个成员分享:
  - 项目最让你印象深刻的部分？
  - 发现了哪些潜在问题或改进点？
  - 你觉得需要补充的功能？
- 汇总问题清单
- 确定优先级

---

## ❓ 待确认事项

1. **Tushare token**: 是否有可用的 Tushare token 用于数据同步测试？需要 Daniel 提供或配置。
2. **GitHub Project Board**: Daniel 提到 backlog 为空，是否需要我来创建初始的 Issue/Feature 列表？
3. **部署环境**: 是否有生产/测试环境？还是完全本地开发？
4. **测试账户**: 是否有预设的用户账号用于前端测试？

---

## 📊 初步印象

**优点**:
- 架构现代化，前后端分离彻底
- 容器化做得好，部署简单
- 文档齐全，有 API 文档、数据库设计、测试指南
- 代码组织清晰，领域驱动
- 测试基础设施完善 (Vitest + Playwright)

**可能需要加强**:
- 后端单元测试覆盖率不详，需检查
- 前端 E2E 测试需要 Playwright 浏览器环境
- 数据同步部分依赖 Tushare token，新成员上手需配置
- 没有看到明确的部署到 cloud 的指南 (只看到 docker-compose)

---

## 🚀 建议的下一步

待团队熟悉完成后，建议讨论:

1. **MVP 功能完备性**: 目前是否已满足最小可用产品需求？
2. **性能优化**: 大规模回测的性能瓶颈？
3. **用户体验**: 是否有明显卡顿或反人类设计？
4. **监控与告警**: 是否需要添加？
5. **多用户支持**: 目前是单用户还是多租户？
6. **策略分享/社区**: 是否要加？
7. **移动端支持**: 是否需要？

---

我将把此报告发布到群组，并安排团队成员开始熟悉。  
同时我会更新 `memory/2026-02-26.md` 记录今天的工作。
