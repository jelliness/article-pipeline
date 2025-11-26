"""
Pipeline Monitoring Model - Real-time pipeline metrics
"""
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import os
import redis
from .database import Database


class PipelineMonitor:
    """Monitor pipeline metrics and Redis queue status"""
    
    def __init__(self):
        self.db = Database()
        self.redis_client: Optional[redis.Redis] = None
        self._init_redis()
    
    def _init_redis(self):
        """Initialize Redis connection"""
        try:
            import os
            redis_host = os.getenv('REDIS_HOST', 'localhost')
            redis_port = int(os.getenv('REDIS_PORT', 6379))
            redis_db = int(os.getenv('REDIS_DB', 0))
            
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=2
            )
            # Test connection
            self.redis_client.ping()
        except Exception as e:
            print(f"Redis connection failed: {e}")
            self.redis_client = None
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """Get Redis queue statistics"""
        if not self.redis_client:
            return {
                "redis_connected": False,
                "total_pending": 0,
                "queues": {
                    "high": 0,
                    "medium": 0,
                    "low": 0
                }
            }
        
        try:
            queue_high = os.getenv('REDIS_QUEUE_HIGH', 'articles:high')
            queue_medium = os.getenv('REDIS_QUEUE_MEDIUM', 'articles:medium')
            queue_low = os.getenv('REDIS_QUEUE_LOW', 'articles:low')
            
            high_count = self.redis_client.llen(queue_high)
            medium_count = self.redis_client.llen(queue_medium)
            low_count = self.redis_client.llen(queue_low)
            
            return {
                "redis_connected": True,
                "total_pending": high_count + medium_count + low_count,
                "queues": {
                    "high": high_count,
                    "medium": medium_count,
                    "low": low_count
                }
            }
        except Exception as e:
            print(f"Error getting queue stats: {e}")
            return {
                "redis_connected": False,
                "total_pending": 0,
                "queues": {
                    "high": 0,
                    "medium": 0,
                    "low": 0
                }
            }
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics from MongoDB"""
        try:
            collection = self.db.get_collection("articles")
            
            # Get counts by status
            total = collection.count_documents({})
            completed = collection.count_documents({"status": "completed"})
            failed = collection.count_documents({"status": "failed"})
            processing = collection.count_documents({"status": "processing"})
            pending = collection.count_documents({"status": "pending"})
            
            # Calculate success rate
            processed = completed + failed
            success_rate = (completed / processed * 100) if processed > 0 else 0
            
            # Get recent activity (last hour)
            one_hour_ago = datetime.utcnow() - timedelta(hours=1)
            recent_completed = collection.count_documents({
                "status": "completed",
                "updated_at": {"$gte": one_hour_ago}
            })
            recent_failed = collection.count_documents({
                "status": "failed",
                "updated_at": {"$gte": one_hour_ago}
            })
            
            # Calculate processing speed (items per minute in last hour)
            recent_total = recent_completed + recent_failed
            processing_speed = recent_total / 60 if recent_total > 0 else 0
            
            # Get average processing time for completed articles
            pipeline = [
                {"$match": {"status": "completed", "scraped_at": {"$exists": True}}},
                {"$project": {
                    "processing_time": {
                        "$subtract": ["$scraped_at", "$created_at"]
                    }
                }},
                {"$group": {
                    "_id": None,
                    "avg_time": {"$avg": "$processing_time"}
                }}
            ]
            avg_result = list(collection.aggregate(pipeline))
            avg_processing_time = avg_result[0]["avg_time"] / 1000 if avg_result else 0  # Convert to seconds
            
            return {
                "total_articles": total,
                "completed": completed,
                "failed": failed,
                "processing": processing,
                "pending": pending,
                "success_rate": round(success_rate, 2),
                "recent_activity": {
                    "last_hour_completed": recent_completed,
                    "last_hour_failed": recent_failed,
                    "processing_speed": round(processing_speed, 2)  # items per minute
                },
                "avg_processing_time": round(avg_processing_time, 2)  # seconds
            }
        except Exception as e:
            print(f"Error getting processing stats: {e}")
            return {
                "total_articles": 0,
                "completed": 0,
                "failed": 0,
                "processing": 0,
                "pending": 0,
                "success_rate": 0,
                "recent_activity": {
                    "last_hour_completed": 0,
                    "last_hour_failed": 0,
                    "processing_speed": 0
                },
                "avg_processing_time": 0
            }
    
    def get_pipeline_health(self) -> Dict[str, Any]:
        """Get overall pipeline health status"""
        queue_stats = self.get_queue_stats()
        processing_stats = self.get_processing_stats()
        
        # Determine health status
        redis_ok = queue_stats["redis_connected"]
        mongodb_ok = self.db.is_connected
        
        # Check for stale processing jobs (processing for > 10 minutes)
        try:
            collection = self.db.get_collection("articles")
            ten_minutes_ago = datetime.utcnow() - timedelta(minutes=10)
            stale_jobs = collection.count_documents({
                "status": "processing",
                "updated_at": {"$lte": ten_minutes_ago}
            })
        except:
            stale_jobs = 0
        
        # Overall health
        if redis_ok and mongodb_ok and stale_jobs == 0:
            health = "healthy"
        elif mongodb_ok:
            health = "degraded"
        else:
            health = "unhealthy"
        
        return {
            "health": health,
            "components": {
                "redis": "connected" if redis_ok else "disconnected",
                "mongodb": "connected" if mongodb_ok else "disconnected"
            },
            "stale_jobs": stale_jobs,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def get_comprehensive_stats(self) -> Dict[str, Any]:
        """Get all monitoring statistics"""
        return {
            "queue": self.get_queue_stats(),
            "processing": self.get_processing_stats(),
            "health": self.get_pipeline_health()
        }
