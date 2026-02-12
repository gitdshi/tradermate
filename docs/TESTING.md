# Testing Overview

Core testing documentation and quick references for running the automated test suite.

- Unit & Integration: Vitest (see `tradermate-portal` folder for test files)
- E2E: Playwright (see `docs/frontend/E2E_README.md` for scenarios and commands)
- Test summaries: `docs/frontend/TEST_SUMMARY.md`

Quick commands:

```bash
# Install deps and Playwright browsers
npm install
npx playwright install

# Run unit tests
npm run test:run

# Run coverage
npm run test:coverage

# Run E2E tests
npm run test:e2e
```

Developer note: if you modify worker task code (backend `app/worker/service/*`), restart the RQ worker using the lifecycle script to ensure the new code is loaded:

```bash
cd /Users/mac/Workspace/Projects/TraderMate/tradermate
./scripts/worker_service.sh restart
```

For full testing guidance and examples, open `docs/frontend/TEST_SUMMARY.md` and `docs/frontend/E2E_README.md`.
