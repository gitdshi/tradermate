"""Worker entry point script (moved to `app.worker`)."""
import os
import sys
from pathlib import Path

# Ensure project root is importable
# Path is: tradermate/app/worker/run_worker.py
# Need to go up 2 levels to get to tradermate/
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Change to project root
os.chdir(ROOT)

from dotenv import load_dotenv
load_dotenv()

from app.api.logging_setup import configure_logging, get_logger  # noqa: E402
configure_logging()
logger = get_logger(__name__)

# Import to register tasks
from app.worker import tasks  # noqa
from app.worker.config import redis_conn, QUEUES

if __name__ == "__main__":
    from rq import Worker
    queue_names = sys.argv[1:] if len(sys.argv) > 1 else ['backtest', 'optimization', 'default']
    queues = [QUEUES[name] for name in queue_names if name in QUEUES]
    if not queues:
        logger.error("No valid queues specified. Available queues: %s", list(QUEUES.keys()))
        sys.exit(1)

    logger.info("Starting worker for queues: %s", queue_names)
    worker = Worker(queues, connection=redis_conn)
    worker.work(with_scheduler=True)
