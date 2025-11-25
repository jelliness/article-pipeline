"""
Publisher module for reading articles and pushing to Redis queues.
"""
import json
import logging
from typing import List, Dict, Any, Set
from utils.config import Config
from database.redis_client import RedisClient
from utils.enricher import MetadataEnricher

logger = logging.getLogger(__name__)


class ArticlePublisher:
    """Publisher that reads articles from JSON and pushes them to Redis queues."""
    
    def __init__(self, redis_client: RedisClient, json_path: str = None):
        """
        Initialize the publisher.
        
        Args:
            redis_client: RedisClient instance
            json_path: Path to articles JSON file
        """
        self.redis_client = redis_client
        self.json_path = json_path or Config.ARTICLES_JSON_PATH
        self.seen_hashes: Set[str] = set()  
        logger.info(f"Publisher initialized with JSON path: {self.json_path}")
    
    def load_articles(self) -> List[Dict[str, Any]]:
        """
        Load articles from JSON file.
        
        Returns:
            List of article dictionaries
        """
        try:
            with open(self.json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                articles = data.get('articles', [])
                logger.info(f"Loaded {len(articles)} articles from JSON")
                return articles
        except FileNotFoundError:
            logger.error(f"Articles file not found: {self.json_path}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format: {e}")
            return []
        except Exception as e:
            logger.error(f"Error loading articles: {e}")
            return []
    
    def publish_articles(self) -> Dict[str, int]:
        """
        Publish all articles to Redis queues based on priority.
        
        Returns:
            Dictionary with counts of published articles per priority
        """
        articles = self.load_articles()
        if not articles:
            logger.warning("No articles to publish")
            return {}
        
        stats = {'high': 0, 'medium': 0, 'low': 0, 'failed': 0, 'duplicates': 0, 'enriched': 0}
        
        for article in articles:
            try:
                # Validate required fields
                required_fields = ['id', 'url', 'source', 'category', 'priority']
                if not all(field in article for field in required_fields):
                    logger.error(f"Article missing required fields: {article}")
                    stats['failed'] += 1
                    continue
                
                # Enrich metadata (Data Processing)
                enriched_article = MetadataEnricher.enrich_metadata(article)
                stats['enriched'] += 1
                
                # Check for duplicates using URL hash
                url_hash = enriched_article.get('url_hash')
                if MetadataEnricher.is_duplicate(url_hash, self.seen_hashes):
                    logger.warning(f"Duplicate URL detected: {article['id']} - {article['url']}")
                    stats['duplicates'] += 1
                    continue
                
                self.seen_hashes.add(url_hash)
                
                # Get the appropriate queue based on priority
                priority = enriched_article.get('priority', 'medium').lower()
                queue_name = Config.get_queue_by_priority(priority)
                
                # Prepare task data with enriched metadata
                task_data = {
                    'id': enriched_article['id'],
                    'url': enriched_article['url'],  
                    'url_original': enriched_article['url_original'],
                    'url_hash': enriched_article['url_hash'],
                    'source': enriched_article['source'],
                    'category': enriched_article['category'],
                    'priority': priority,
                    'domain': enriched_article['domain'],
                    'published_at': enriched_article['published_at'],
                    'enriched': enriched_article['enriched'],
                    'user_id': enriched_article.get('user_id')  
                }
                
                # Push to Redis queue
                if self.redis_client.push_to_queue(queue_name, task_data):
                    stats[priority] += 1
                    logger.info(f"Published article {article['id']} to {priority} priority queue")
                else:
                    stats['failed'] += 1
                    logger.error(f"Failed to publish article {article['id']}")
                    
            except Exception as e:
                logger.error(f"Error publishing article {article.get('id', 'unknown')}: {e}")
                stats['failed'] += 1
        
        # Log summary
        total_published = stats['high'] + stats['medium'] + stats['low']
        logger.info(f"Publishing complete: {total_published} articles published, {stats['failed']} failed")
        logger.info(f"  High priority: {stats['high']}")
        logger.info(f"  Medium priority: {stats['medium']}")
        logger.info(f"  Low priority: {stats['low']}")
        logger.info(f"  Enriched: {stats['enriched']}")
        logger.info(f"  Duplicates skipped: {stats['duplicates']}")
        
        return stats
    
    def publish_single_article(self, article: Dict[str, Any]) -> bool:
        """
        Publish a single article to the appropriate queue with enrichment.
        
        Args:
            article: Article dictionary with 'id' or 'article_id' field
            
        Returns:
            bool: True if published successfully
        """
        try:
            # Enrich metadata
            enriched_article = MetadataEnricher.enrich_metadata(article)
            
            # Check for duplicates
            url_hash = enriched_article.get('url_hash')
            if MetadataEnricher.is_duplicate(url_hash, self.seen_hashes):
                article_id = enriched_article.get('id') or enriched_article.get('article_id', 'unknown')
                logger.warning(f"Duplicate URL detected: {article_id}")
                return False
            
            self.seen_hashes.add(url_hash)
            
            priority = enriched_article.get('priority', 'medium').lower()
            queue_name = Config.get_queue_by_priority(priority)
            
            # Support both 'id' and 'article_id' fields
            article_id = enriched_article.get('id') or enriched_article.get('article_id', 'unknown')
            
            task_data = {
                'article_id': article_id,
                'url': enriched_article['url'],
                'url_original': enriched_article['url_original'],
                'url_hash': enriched_article['url_hash'],
                'source': enriched_article['source'],
                'category': enriched_article['category'],
                'priority': priority,
                'domain': enriched_article['domain'],
                'published_at': enriched_article['published_at'],
                'enriched': enriched_article['enriched'],
                'user_id': enriched_article.get('user_id')  # Pass through user context for multi-tenancy
            }
            
            return self.redis_client.push_to_queue(queue_name, task_data)
        except Exception as e:
            logger.error(f"Error publishing single article: {e}")
            return False
    
    def get_queue_stats(self) -> Dict[str, int]:
        """
        Get current queue lengths.
        
        Returns:
            Dictionary with queue lengths
        """
        queues = [
            Config.REDIS_QUEUE_HIGH,
            Config.REDIS_QUEUE_MEDIUM,
            Config.REDIS_QUEUE_LOW
        ]
        return self.redis_client.get_all_queue_lengths(queues)
