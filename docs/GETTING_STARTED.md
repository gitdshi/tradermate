# Getting Started — TraderMate

Quick instructions to get a local development environment running.

## Prerequisites
- Docker & Docker Compose
- Python 3.11
- Node.js 18+ and npm

## Backend (API)

1. Copy environment template:

```bash
cp .env.example .env
# Edit .env to add your Tushare token and credentials
```

2. Start MySQL (Docker):

```bash
docker-compose up -d mysql
```

3. Install Python dependencies and run API:

```bash
pip install -r requirements.txt
# Use the lifecycle script to run the API (ensures venv and logs)
./scripts/api_service.sh start|stop|restart|status
# For quick manual debugging only:
python -m uvicorn app.api.main:app --reload
```

## Frontend

1. Install and run the frontend (now in `tradermate-portal`):

```bash
cd tradermate-portal
npm install
# Use the lifecycle script for the frontend dev server:
./scripts/portal_service.sh start|stop|restart|status
```

2. The frontend runs at http://localhost:5173 and expects the backend at http://localhost:8000 by default.

## Testing

- Unit & integration tests are configured with Vitest. See `docs/TESTING.md` and `docs/frontend/TEST_SUMMARY.md` for commands.
- E2E tests use Playwright; ensure `npx playwright install` has been run before executing E2E tests.
