# TraderMate API

FastAPI-based REST API for the TraderMate trading platform.

## Features

- **Authentication**: JWT-based user authentication
- **Strategy Management**: CRUD operations for trading strategies
- **Backtesting**: Single and batch backtest execution
- **Market Data**: Access to stock history, indicators, and market overview
- **Background Jobs**: Async task execution for long-running operations

## Quick Start

### Local Development

1. **Install dependencies:**
```bash
pip install -r requirements-api.txt
```

2. **Start MySQL (if not running):**
```bash
docker-compose up -d mysql
```

3. **Run the API server:**
```bash
# Using the startup script
./scripts/api_service.sh start|stop|restart|status

# Or manually
python -m uvicorn app.api.main:app --reload
```

4. **Access the API:**
- API: http://localhost:8000
- Interactive Docs: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

### Docker Deployment

```bash
# Build and start all services
docker-compose up -d

# View logs
docker-compose logs -f api

# Stop services
docker-compose down
```

## API Endpoints

For a detailed per-endpoint reference, see the API pages under `docs/api/`:

- **Authentication**: [docs/api/auth.md](api/auth.md)
- **Strategies**: [docs/api/strategies.md](api/strategies.md)
- **Market Data**: [docs/api/data.md](api/data.md)
- **Backtesting**: [docs/api/backtest.md](api/backtest.md)
- **Queue & Jobs**: [docs/api/queue.md](api/queue.md)
- **Optimization**: [docs/api/optimization.md](api/optimization.md)

Open the relevant file above for usage examples, request/response schemas, and notes.

## Configuration

Environment variables (see `docker-compose.yml` or create `.env` file):

```bash
# App Settings
APP_NAME=TraderMate
APP_VERSION=1.0.0
DEBUG=true

# Database
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=password
TUSHARE_DATABASE=tushare
VNPY_DATABASE=vnpy

# Redis (for job queue)
REDIS_HOST=localhost
REDIS_PORT=6379

# JWT
JWT_SECRET_KEY=your-secret-key-change-in-production
JWT_ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30
REFRESH_TOKEN_EXPIRE_DAYS=7

# CORS
CORS_ORIGINS=http://localhost:3000,http://localhost:5173
```

## Architecture

```
app/api/
├── main.py              # FastAPI app entry point
├── config.py            # Settings and configuration
├── models/              # Pydantic models
│   ├── user.py         # User models
│   ├── strategy.py     # Strategy models
│   └── backtest.py     # Backtest models
├── routes/              # API endpoints
│   ├── auth.py         # Authentication routes
│   ├── strategies.py   # Strategy CRUD
│   ├── data.py         # Market data routes
│   └── backtest.py     # Backtest routes
├── middleware/          # Custom middleware
│   └── auth.py         # JWT authentication
└── services/            # Business logic
    ├── db.py           # Database connections
    ├── data_service.py # Market data service
    ├── backtest_service.py  # Backtest execution
    └── strategy_service.py  # Strategy validation

> Note: Several service implementations have been reorganized into canonical `service/` subpackages (e.g. `app/datasync/service`, `app/worker/service`). For running and managing long-lived processes (API, worker, datasync), use the lifecycle scripts in `tradermate/scripts/` (e.g. `api_service.sh`, `worker_service.sh`, `datasync_service.sh`).
```

## Testing

```bash

# Test basic endpoints (use curl/Postman; legacy `scripts/test_api.py` removed)
# Example:
# curl -sS -X GET http://localhost:8000/

# Run all tests (when implemented)
pytest tests/
```

## Example Usage

### Register and Login

```bash
# Register
curl -X POST http://localhost:8000/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"trader1","email":"trader1@example.com","password":"secret123"}'

# Login
curl -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"trader1","password":"secret123"}'
```

### Run Backtest

```bash
# Submit backtest (use token from login)
curl -X POST http://localhost:8000/api/backtest \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "strategy_id": 1,
    "symbol": "000001.SZ",
    "start_date": "2020-01-01",
    "end_date": "2023-12-31",
    "initial_capital": 100000.0
  }'

# Check status
curl http://localhost:8000/api/backtest/JOB_ID \
  -H "Authorization: Bearer YOUR_TOKEN"
```

## Development

### Adding New Endpoints

1. Create Pydantic models in `app/api/models/`
2. Add route handlers in `app/api/routes/`
3. Implement business logic in `app/api/services/`
4. Include router in `app/api/main.py`

### Adding Background Tasks

Use FastAPI's `BackgroundTasks` for short tasks:

```python
from fastapi import BackgroundTasks

@router.post("/task")
def create_task(background_tasks: BackgroundTasks):
    background_tasks.add_task(my_function, arg1, arg2)
    return {"status": "processing"}
```

For long-running tasks, use Redis + RQ (Phase 2).

## Troubleshooting

### Database Connection Error

Make sure MySQL is running:
```bash
docker-compose up -d mysql
# Or start local MySQL service
```

### Port Already in Use

```bash
# Kill existing process
lsof -ti:8000 | xargs kill -9

# Or use a different port
uvicorn app.api.main:app --port 8001
```

### Module Import Errors

```bash
# Reinstall dependencies
pip install -r requirements-api.txt

# Check Python path
python -c "import sys; print(sys.path)"
```

## Next Steps

- [ ] Phase 2: Redis + RQ for background job processing
- [ ] Phase 3: Frontend React application
- [ ] Phase 4: Strategy editor and management UI
- [ ] Phase 5: K-Line charts and technical analysis
- [ ] Phase 6: Advanced features (alerts, portfolio, etc.)

## License

See main project LICENSE file.
