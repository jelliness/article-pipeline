"""
Caching layer for scraped articles to avoid unnecessary reprocessing.
"""
import logging
import hashlib
import json
from typing import Optional, Dict, Any
from datetime import datetime
from .base import Base
from .config import Config

logger = logging.getLogger(__name__)


class ArticleCache(metaclass=Base):
    """
    Cache layer for tracking scraped articles. Shared across the application.
    
    Uses Redis for fast lookups and MongoDB for persistent storage.
    Prevents unnecessary scraping of unchanged URLs.
    """
    
    def __init__(self, redis_client=None, mongodb_client=None):
        """
        Initialize cache with Redis and MongoDB clients.
        
        Args:
            redis_client: RedisClient instance
            mongodb_client: ArticleDatabase instance
        """
        self.redis = redis_client
        self.mongodb = mongodb_client
        self.enabled = Config.CACHE_ENABLED
        self.ttl = Config.CACHE_TTL
        self.prefix = Config.CACHE_PREFIX
        
        logger.info(f"Article cache initialized (enabled={self.enabled}, ttl={self.ttl}s)")
    
    def _generate_cache_key(self, url: str) -> str:
        """
        Generate cache key from URL hash.
        
        Args:
            url: Article URL
            
        Returns:
            Cache key with prefix
        """
        url_hash = hashlib.sha256(url.encode('utf-8')).hexdigest()
        return f"{self.prefix}{url_hash}"
    
    def check_redis_cache(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Check if URL exists in Redis cache.
        
        Args:
            url: Article URL
            
        Returns:
            Cached article data or None
        """
        if not self.enabled or not self.redis:
            return None
        
        try:
            cache_key = self._generate_cache_key(url)
            cached_data = self.redis.client.get(cache_key)
            
            if cached_data:
                data = json.loads(cached_data)
                logger.debug(f"Redis cache HIT for {url}")
                return data
            
            logger.debug(f"Redis cache MISS for {url}")
            return None
            
        except Exception as e:
            logger.warning(f"Redis cache check failed: {e}")
            return None
    
    def check_mongodb_cache(self, url: str, url_hash: str) -> Optional[Dict[str, Any]]:
        """
        Check if URL exists in MongoDB.
        
        Args:
            url: Article URL
            url_hash: Pre-computed URL hash from metadata enrichment
            
        Returns:
            Existing article data or None
        """
        if not self.enabled or not self.mongodb:
            return None
        
        try:
            # Try to find by url_hash (more reliable due to normalization)
            article = self.mongodb.articles.find_one(
                {'url_hash': url_hash},
                {'_id': 0}  # Exclude MongoDB _id
            )
            
            if article:
                logger.debug(f"MongoDB cache HIT for {url}")
                return article
            
            logger.debug(f"MongoDB cache MISS for {url}")
            return None
            
        except Exception as e:
            logger.warning(f"MongoDB cache check failed: {e}")
            return None
    
    def should_scrape(self, url: str, url_hash: str, force: bool = False) -> Dict[str, Any]:
        """
        Determine if URL should be scraped.
        
        Checks both Redis and MongoDB for existing data.
        
        Args:
            url: Article URL
            url_hash: Pre-computed URL hash
            force: Force scraping even if cached
            
        Returns:
            Dictionary with decision:
            {
                'should_scrape': bool,
                'reason': str,
                'cached_data': dict or None,
                'cache_source': 'redis' | 'mongodb' | None
            }
        """
        if force:
            return {
                'should_scrape': True,
                'reason': 'forced',
                'cached_data': None,
                'cache_source': None
            }
        
        if not self.enabled:
            return {
                'should_scrape': True,
                'reason': 'cache_disabled',
                'cached_data': None,
                'cache_source': None
            }
        
        # Check Redis first (faster)
        redis_data = self.check_redis_cache(url)
        if redis_data:
            return {
                'should_scrape': False,
                'reason': 'redis_cache_hit',
                'cached_data': redis_data,
                'cache_source': 'redis'
            }
        
        # Check MongoDB (persistent storage)
        mongodb_data = self.check_mongodb_cache(url, url_hash)
        if mongodb_data:
            # Update Redis cache for future lookups
            self.set_redis_cache(url, mongodb_data)
            
            return {
                'should_scrape': False,
                'reason': 'mongodb_cache_hit',
                'cached_data': mongodb_data,
                'cache_source': 'mongodb'
            }
        
        # No cache found, should scrape
        return {
            'should_scrape': True,
            'reason': 'no_cache',
            'cached_data': None,
            'cache_source': None
        }
    
    def set_redis_cache(self, url: str, article_data: Dict[str, Any]) -> bool:
        """
        Store article data in Redis cache.
        
        Args:
            url: Article URL
            article_data: Article data to cache
            
        Returns:
            True if cached successfully
        """
        if not self.enabled or not self.redis:
            return False
        
        try:
            cache_key = self._generate_cache_key(url)
            
            # Prepare cache data (exclude content to save memory)
            cache_data = {
                'article_id': article_data.get('article_id'),
                'url': article_data.get('url'),
                'url_hash': article_data.get('url_hash'),
                'title': article_data.get('title'),
                'source': article_data.get('source'),
                'category': article_data.get('category'),
                'scrape_status': article_data.get('scrape_status'),
                'cached_at': datetime.utcnow().isoformat()
            }
            
            # Store with TTL
            self.redis.client.setex(
                cache_key,
                self.ttl,
                json.dumps(cache_data)
            )
            
            logger.debug(f"Cached article in Redis: {url} (TTL={self.ttl}s)")
            return True
            
        except Exception as e:
            logger.warning(f"Failed to cache in Redis: {e}")
            return False
    
    def close(self):
        """Close cache connections (handled by shared instances)."""
        logger.info("Cache connections closed")
