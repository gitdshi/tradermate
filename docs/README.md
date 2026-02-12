# TraderMate Documentation

Welcome to the central documentation hub for TraderMate — a comprehensive trading platform built on top of vn.py and a FastAPI backend, with a React/Vite frontend, automated data ingestion from Tushare, and containerized deployment.

This `docs/` directory consolidates all project documentation: architecture, APIs, deployment, testing, developer guides, and feature notes.

## Quick Links
- **Project overview**: this file
- **Getting started & run instructions**: [GETTING_STARTED](GETTING_STARTED.md) (short guide below)
- **API documentation**: [API_README.md](API_README.md)
- **Testing & QA**: [TESTING.md](TESTING.md)
- **Implementation status & phases**: [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md)
- **Background job / Phase 2**: [PHASE2_COMPLETE.md](PHASE2_COMPLETE.md)
- **Database & Tushare**: [DATABASE_ARCHITECTURE.md](DATABASE_ARCHITECTURE.md), [tushare_setup.md](tushare_setup.md), [tushare_stock_endpoints.md](tushare_stock_endpoints.md)
- **Frontend test artifacts & summaries**: [docs/frontend/TEST_SUMMARY.md](docs/frontend/TEST_SUMMARY.md)

## Project Summary

TraderMate provides:

- Strategy management (create/update/delete user strategies)
- Market data access (symbol lists, OHLCV history, indicators)
- Backtesting (single, batch, queued/async execution)
- Parameter optimization and comparison tools
- Portfolio and risk analytics dashboard
- Authentication (JWT) and user management
- Background job processing via Redis + RQ (Phase 2)
- Comprehensive unit/integration/E2E testing (Vitest + Playwright)

## Where to Start

1. Read the quick start in [GETTING_STARTED.md](GETTING_STARTED.md).
2. Explore the API docs in [API_README.md](API_README.md) or open the running server's Swagger UI at `http://localhost:8000/docs`.
3. Run unit tests and E2E tests as described in [TESTING.md](TESTING.md).

## Project Structure (high level)

Repository root layout (top-level `app/`, `tradermate-portal/`, `mysql/`, `scripts/`, `docs/`):

```
tradermate/
├── app/                   # Backend (FastAPI) + services
├── tradermate-portal/     # React + Vite app (UI)
├── docs/                  # Consolidated documentation (this folder)
├── mysql/                 # MySQL init scripts and volumes
├── scripts/               # Helper scripts (start, worker, test)
├── docker-compose.yml     # Multi-service compose file
├── Dockerfile             # App container image
└── requirements.txt       # Python dependencies
```

For a detailed directory map and API routes, see [API_README.md](API_README.md) and [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md).

## Current Status (2026-02-12)

- Canonical service implementations moved into `service/` subpackages:
	- `app/datasync/service/` — Data ingestion and sync daemons (Tushare/AkShare helpers).
	- `app/worker/service/` — Worker tasks and run logic.
	- Package roots (`app/datasync/`, `app/worker/`) now contain minimal `main.py` entrypoints only.
- Infrastructure centralized under `app/infrastructure/` (`config/`, `logging/`, `db/`).
- Operational scripts renamed and standardized to `*_service.sh` and located in `tradermate/scripts/`:
	- `api_service.sh`, `worker_service.sh`, `datasync_service.sh` (backend)
	- `tradermate-portal/scripts/portal_service.sh` (frontend)
	- Logs and PID files are written to `tradermate/logs/` and `tradermate-portal/logs/` respectively.
- `.vite` cache is ignored in `tradermate-portal/.gitignore` and has been removed from the workspace.
- Contributor/operations principle added: read `docs/OPERATIONS_GUIDE.md` before creating or moving code; use service scripts to manage long-running processes.

### Quick service commands

Backend (from `tradermate/`):
```
./scripts/api_service.sh start|stop|restart|status
./scripts/worker_service.sh start|stop|restart|status [queues...]
./scripts/datasync_service.sh start|stop|restart|status
```

Frontend (from `tradermate-portal/`):
```
./scripts/portal_service.sh start|stop|restart|status
```

If you modify worker task code, always restart the worker using the scripts above to avoid stale imports.

## Contributing & Support

Please follow the contributing steps in `IMPLEMENTATION_STATUS.md` and include tests for new features. If you have questions, open an issue in the repository.

---

> Note: The top-level README has been moved into `docs/`. A pointer remains at the repository root to this documentation hub.
