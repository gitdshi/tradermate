"""Redis-based Job Storage and Management.

Renamed from `job_storage.py` to follow `{domain}_service.py` convention.
"""
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import json
from redis import Redis
from rq.job import Job
from rq import Queue

from app.api.config import get_settings

settings = get_settings()


class JobStorage:
    """Redis-based job storage and management."""
    
    def __init__(self):
        """Initialize Redis connection."""
        self.redis = Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            decode_responses=True  # Decode strings for metadata
        )
        self.prefix = "tradermate:job:"
        self.result_prefix = "tradermate:result:"
        self.ttl = 86400 * 7  # Keep results for 7 days
    
    def save_job_metadata(self, job_id: str, metadata: Dict[str, Any]) -> None:
        key = f"{self.prefix}{job_id}"
        metadata["updated_at"] = datetime.now().isoformat()
        self.redis.setex(key, self.ttl, json.dumps(metadata))
    
    def get_job_metadata(self, job_id: str) -> Optional[Dict[str, Any]]:
        key = f"{self.prefix}{job_id}"
        data = self.redis.get(key)
        return json.loads(data) if data else None
    
    def save_result(self, job_id: str, result: Dict[str, Any]) -> None:
        key = f"{self.result_prefix}{job_id}"
        try:
            metadata = self.get_job_metadata(job_id)
            if metadata and "parameters" in metadata and "parameters" not in result:
                result = dict(result)
                result["parameters"] = metadata.get("parameters", {})
        except Exception:
            pass

        self.redis.setex(key, self.ttl, json.dumps(result))
    
    def get_result(self, job_id: str) -> Optional[Dict[str, Any]]:
        key = f"{self.result_prefix}{job_id}"
        data = self.redis.get(key)
        return json.loads(data) if data else None
    
    def update_job_status(self, job_id: str, status: str, **kwargs) -> None:
        metadata = self.get_job_metadata(job_id)
        if metadata:
            metadata["status"] = status
            metadata.update(kwargs)
            self.save_job_metadata(job_id, metadata)
    
    def update_progress(self, job_id: str, progress: float, message: str = "") -> None:
        metadata = self.get_job_metadata(job_id)
        if metadata:
            metadata["progress"] = progress
            if message:
                metadata["progress_message"] = message
            self.save_job_metadata(job_id, metadata)
    
    def list_user_jobs(
        self, 
        user_id: int, 
        status: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        pattern = f"{self.prefix}*"
        jobs = []
        
        for key in self.redis.scan_iter(match=pattern, count=100):
            data = self.redis.get(key)
            if data:
                metadata = json.loads(data)
                if metadata.get("user_id") == user_id:
                    if status is None or metadata.get("status") == status:
                        jobs.append(metadata)
        
        jobs.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return jobs[:limit]
    
    def delete_job(self, job_id: str) -> bool:
        meta_key = f"{self.prefix}{job_id}"
        result_key = f"{self.result_prefix}{job_id}"
        deleted = self.redis.delete(meta_key, result_key)
        return deleted > 0
    
    def cancel_job(self, job_id: str, queue: Queue) -> bool:
        try:
            job = Job.fetch(job_id, connection=self.redis)
            if job.get_status() in ['queued', 'started']:
                job.cancel()
                self.update_job_status(job_id, 'cancelled')
                return True
            return False
        except Exception as e:
            print(f"Error cancelling job {job_id}: {e}")
            return False
    
    def cleanup_old_jobs(self, days: int = 7) -> int:
        cutoff = datetime.now() - timedelta(days=days)
        pattern = f"{self.prefix}*"
        deleted = 0
        
        for key in self.redis.scan_iter(match=pattern, count=100):
            data = self.redis.get(key)
            if data:
                metadata = json.loads(data)
                created_at = datetime.fromisoformat(metadata.get("created_at", ""))
                
                if created_at < cutoff:
                    job_id = metadata.get("job_id")
                    if self.delete_job(job_id):
                        deleted += 1
        
        return deleted
    
    def get_queue_stats(self) -> Dict[str, Any]:
        from app.worker.config import QUEUES
        stats = {}
        for name, queue in QUEUES.items():
            stats[name] = {
                "queued": len(queue),
                "failed": queue.failed_job_registry.count,
                "finished": queue.finished_job_registry.count,
                "started": queue.started_job_registry.count,
            }
        return stats


# Singleton instance
_job_storage = None


def get_job_storage() -> JobStorage:
    """Get JobStorage singleton instance."""
    global _job_storage
    if _job_storage is None:
        _job_storage = JobStorage()
    return _job_storage
