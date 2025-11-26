"""
Redis client wrapper for queue operations.
"""
import json
import logging
from typing import Optional, Dict, Any
import redis
from redis.exceptions import RedisError
from utils.base import Base

logger = logging.getLogger(__name__)


class RedisClient(metaclass=Base):
    """Wrapper class for Redis operations. Shared across the application."""
    
    _initialized = False
    
    def __init__(self, host: str, port: int, db: int):
        """
        Initialize Redis client (reuses existing instance if already created).
        
        Args:
            host: Redis server host (from Config)
            port: Redis server port (from Config)
            db: Redis database number (from Config)
        """
        # Prevent re-initialization
        if self._initialized:
            logger.debug("Redis client already initialized")
            return
        
        try:
            self.client = redis.Redis(
                host=host,
                port=port,
                db=db,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                connection_pool=redis.ConnectionPool(
                    host=host,
                    port=port,
                    db=db,
                    decode_responses=True,
                    max_connections=10
                )
            )
            # Test connection
            self.client.ping()
            self._initialized = True
            logger.info(f"Connected to Redis at {host}:{port}")
        except RedisError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def push_to_queue(self, queue_name: str, data: Dict[str, Any]) -> bool:
        """
        Push data to a Redis queue (list).
        
        Args:
            queue_name: Name of the queue
            data: Dictionary to push
            
        Returns:
            bool: True if successful
        """
        try:
            json_data = json.dumps(data)
            self.client.rpush(queue_name, json_data)
            logger.debug(f"Pushed to queue '{queue_name}': {data.get('id', 'unknown')}")
            return True
        except (RedisError, json.JSONDecodeError) as e:
            logger.error(f"Failed to push to queue '{queue_name}': {e}")
            return False
    
    def pop_from_queue(self, queue_name: str, timeout: int = 0) -> Optional[Dict[str, Any]]:
        """
        Pop data from a Redis queue (blocking operation).
        
        Args:
            queue_name: Name of the queue
            timeout: Timeout in seconds (0 = block indefinitely)
            
        Returns:
            Dictionary or None if queue is empty/timeout
        """
        try:
            result = self.client.blpop(queue_name, timeout=timeout)
            if result:
                _, json_data = result
                data = json.loads(json_data)
                logger.debug(f"Popped from queue '{queue_name}': {data.get('id', 'unknown')}")
                return data
            return None
        except (RedisError, json.JSONDecodeError) as e:
            logger.error(f"Failed to pop from queue '{queue_name}': {e}")
            return None
    
    def pop_from_queues(self, queue_names: list, timeout: int = 0) -> Optional[Dict[str, Any]]:
        """
        Pop data from multiple Redis queues with priority (first queue has highest priority).
        
        Args:
            queue_names: List of queue names (in priority order)
            timeout: Timeout in seconds (0 = block indefinitely)
            
        Returns:
            Dictionary or None if queues are empty/timeout
        """
        try:
            result = self.client.blpop(queue_names, timeout=timeout)
            if result:
                queue_name, json_data = result
                data = json.loads(json_data)
                logger.debug(f"Popped from queue '{queue_name}': {data.get('id', 'unknown')}")
                return data
            return None
        except (RedisError, json.JSONDecodeError) as e:
            logger.error(f"Failed to pop from queues: {e}")
            return None
    
    def get_queue_length(self, queue_name: str) -> int:
        """
        Get the length of a queue.
        
        Args:
            queue_name: Name of the queue
            
        Returns:
            Queue length or -1 on error
        """
        try:
            return self.client.llen(queue_name)
        except RedisError as e:
            logger.error(f"Failed to get queue length for '{queue_name}': {e}")
            return -1
    
    def get_all_queue_lengths(self, queue_names: list) -> Dict[str, int]:
        """
        Get lengths of multiple queues.
        
        Args:
            queue_names: List of queue names
            
        Returns:
            Dictionary mapping queue names to lengths
        """
        lengths = {}
        for queue_name in queue_names:
            lengths[queue_name] = self.get_queue_length(queue_name)
        return lengths
    
    def close(self):
        """Close the Redis connection."""
        if self.client:
            self.client.close()
            logger.info("Redis connection closed")
