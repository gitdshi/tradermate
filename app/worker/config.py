"""Worker and Queue Configuration."""
from redis import Redis
from rq import Queue
from app.api.config import get_settings

settings = get_settings()

# Redis connection
redis_conn = Redis(
    host=settings.redis_host,
    port=settings.redis_port,
    db=settings.redis_db,
    decode_responses=False  # Keep bytes for job data
)

# Define queues with priorities
QUEUE_HIGH = Queue('high', connection=redis_conn, default_timeout=600)
QUEUE_DEFAULT = Queue('default', connection=redis_conn, default_timeout=1800)
QUEUE_LOW = Queue('low', connection=redis_conn, default_timeout=3600)
QUEUE_BACKTEST = Queue('backtest', connection=redis_conn, default_timeout=3600)
QUEUE_OPTIMIZATION = Queue('optimization', connection=redis_conn, default_timeout=7200)

# Queue registry
QUEUES = {
    'high': QUEUE_HIGH,
    'default': QUEUE_DEFAULT,
    'low': QUEUE_LOW,
    'backtest': QUEUE_BACKTEST,
    'optimization': QUEUE_OPTIMIZATION,
}


def get_queue(name: str = 'default') -> Queue:
    """Get queue by name."""
    return QUEUES.get(name, QUEUE_DEFAULT)
