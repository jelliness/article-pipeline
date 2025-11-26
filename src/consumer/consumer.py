"""
Consumer module for processing articles from Redis queues.
Multi-threaded by default processing.
"""
import logging
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
from utils.config import Config
from database.redis_client import RedisClient
from utils.cache import ArticleCache
from database.mongodb import ArticleDatabase
from services.scraper import ArticleScraper

logger = logging.getLogger(__name__)


class ArticleConsumer:
    """
    Consumer that processes articles from Redis queues.
    Multi-threaded by default processing.
    """
    
    def __init__(
        self,
        redis_client: RedisClient,
        database: ArticleDatabase,
        scraper: ArticleScraper,
        cache: ArticleCache = None,
        max_workers: int = 10,
        use_threading: bool = True
    ):
        """
        Initialize the consumer.
        
        Args:
            redis_client: RedisClient instance
            database: ArticleDatabase instance
            scraper: ArticleScraper instance
            cache: ArticleCache instance (optional)
            max_workers: Maximum number of concurrent workers (default: 10)
            use_threading: Use multi-threaded processing (default: True)
        """
        self.redis_client = redis_client
        self.database = database
        self.scraper = scraper
        self.cache = cache or ArticleCache(redis_client, database)
        self.max_workers = max_workers
        self.use_threading = use_threading
        self.running = False
        
        # Thread-safe statistics
        self.stats_lock = threading.Lock() if use_threading else None
        self.stats = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'duplicates': 0,
            'cached': 0,
            'concurrent_peak': 0 if use_threading else None
        }
        
        mode = f"multi-threaded with {max_workers} workers" if use_threading else "single-threaded"
        logger.info(f"Consumer initialized ({mode}) with caching layer")
    
    def _update_stats(self, stat_name: str, increment: int = 1):
        """Thread-safe statistics update"""
        if self.stats_lock:
            with self.stats_lock:
                self.stats[stat_name] += increment
        else:
            self.stats[stat_name] += increment
    
    def _get_stats(self) -> dict:
        """Thread-safe statistics retrieval"""
        if self.stats_lock:
            with self.stats_lock:
                return self.stats.copy()
        else:
            return self.stats.copy()
    
    def process_single_article(self, task_data: dict) -> bool:
        """
        Process a single article task (thread-safe).
        
        Args:
            task_data: Task data from Redis
            
        Returns:
            bool: True if processed successfully
        """
        # Support both 'article_id' and 'id' field names
        article_id = task_data.get('article_id') or task_data.get('id', 'unknown')
        url = task_data.get('url')
        
        if not url:
            logger.error(f"Article {article_id}: No URL provided")
            return False
        
        try:
            thread_info = f"[{threading.current_thread().name}] " if self.use_threading else ""
            logger.info(f"{thread_info}Processing article {article_id}")
            
            if not self.use_threading:
                print(f"\n{'='*60}")
                print(f" Processing Article: {article_id}")
                print(f" URL: {url}")
                print(f"{'='*60}")
            
            url_hash = task_data.get('url_hash', '')
            cache_decision = self.cache.should_scrape(url, url_hash)
            
            if not cache_decision['should_scrape']:
                logger.info(
                    f"{thread_info}Article {article_id} found in cache ({cache_decision['cache_source']})"
                )
                # Check if article exists in database
                existing_article = self.database.get_article(article_id)
                
                if existing_article:
                    # Update existing article status to completed
                    self.database.update_article(article_id, {'status': 'completed'})
                else:
                    # Insert article from cache data
                    cached_data = self.cache.check_redis_cache(url)
                    article_data = {
                        'article_id': article_id,
                        'url': url,
                        'source': task_data.get('source'),
                        'category': task_data.get('category'),
                        'priority': task_data.get('priority'),
                        'status': 'completed',
                        'title': cached_data.get('title') if cached_data else None,
                        'content': cached_data.get('content') if cached_data else None,
                        'user_id': task_data.get('user_id')
                    }
                    # Add enriched metadata
                    enriched_fields = [
                        'url_original', 'url_hash', 'domain',
                        'published_at', 'enriched'
                    ]
                    for field in enriched_fields:
                        if field in task_data:
                            article_data[field] = task_data[field]
                    
                    self.database.insert_article(article_data)
                    logger.info(f"{thread_info}Article {article_id} inserted from cache")
                
                self._update_stats('cached')
                self._update_stats('successful')
                return True
            
            # Check if article already exists in database
            existing_article = self.database.get_article(article_id)
            
            if existing_article:
                # If article is already completed/failed, skip scraping
                if existing_article.get('status') in ['completed', 'failed']:
                    logger.info(f"{thread_info}Article {article_id} already processed")
                    self._update_stats('duplicates')
                    return True
                
                # If pending/processing, continue to scrape and update it
                logger.info(f"{thread_info}Article {article_id} exists, will scrape and update")
            
            # Scrape the article
            logger.debug(f"Cache decision: {cache_decision['reason']}, proceeding to scrape")
            from datetime import datetime
            scrape_start = datetime.utcnow()
            scrape_result = self.scraper.scrape_article(url)
            scrape_end = datetime.utcnow()
            
            # Determine article status based on scrape result
            scrape_status = scrape_result.get('status')
            if scrape_status == 'success':
                article_status = 'completed'
            elif scrape_status == 'partial':
                article_status = 'failed' 
            else:  
                article_status = 'failed'
            
            # Prepare article data for database - preserve all enriched metadata
            article_data = {
                'article_id': article_id,
                'url': url,
                'source': task_data.get('source'),
                'category': task_data.get('category'),
                'priority': task_data.get('priority'),
                'status': article_status,
                'title': scrape_result.get('title'),
                'content': scrape_result.get('content'),
                'scrape_status': scrape_result.get('status'),
                'scrape_error': scrape_result.get('error'),
                'scraped_at': scrape_end,  # Timestamp when scraping completed
                'user_id': task_data.get('user_id')  # Store user context for multi-tenancy
            }
            
            # Add enriched metadata if available
            enriched_fields = [
                'url_original', 'url_hash', 'domain',
                'published_at', 'enriched'
            ]
            for field in enriched_fields:
                if field in task_data:
                    article_data[field] = task_data[field]
            
            # Store in database (insert new or update existing)
            if existing_article:
                # Update existing article
                success = self.database.update_article(article_id, article_data)
                operation = "updated"
            else:
                # Insert new article
                success = self.database.insert_article(article_data)
                operation = "inserted"
            
            if success:
                # Cache the article for future lookups
                self.cache.set_redis_cache(url, article_data)
                
                if scrape_result['status'] == 'success':
                    logger.info(f"{thread_info}Article {article_id} completed successfully")
                    if not self.use_threading:
                        print(f" SUCCESS: Article {article_id} {operation} successfully")
                    self._update_stats('successful')
                    return True
                else:
                    logger.warning(f"{thread_info}Article {article_id} completed with errors")
                    if not self.use_threading:
                        print(f" WARNING: Article {article_id} {operation} with scrape errors")
                    self._update_stats('failed')
                    return False
            else:
                logger.error(f"{thread_info}Failed to store article {article_id}")
                if not self.use_threading:
                    print(f" ERROR: Failed to {operation.rstrip('d')} article {article_id}")
                self._update_stats('failed')
                return False
                
        except Exception as e:
            logger.error(f"Error processing article {article_id}: {e}")
            if not self.use_threading:
                print(f" EXCEPTION: Error processing article {article_id}: {e}")
            self._update_stats('failed')
            return False
        finally:
            self._update_stats('processed')
    
    def consume(self, queue_timeout: int = 5, max_iterations: Optional[int] = None):
        """
        Start consuming articles from Redis queues.
        Routes to multi-threaded or single-threaded implementation.
        
        Args:
            queue_timeout: Timeout for blocking pop operation (seconds)
            max_iterations: Maximum number of articles to process (None = unlimited)
        """
        if self.use_threading:
            self._consume_parallel(queue_timeout, max_iterations)
        else:
            self._consume_sequential(queue_timeout, max_iterations)
    
    def _consume_sequential(self, queue_timeout: int = 5, max_iterations: Optional[int] = None):
        """Single-threaded consumption (legacy mode)"""
        self.running = True
        iterations = 0
        
        queues = [
            Config.REDIS_QUEUE_HIGH,
            Config.REDIS_QUEUE_MEDIUM,
            Config.REDIS_QUEUE_LOW
        ]
        
        logger.info("Consumer started (single-threaded)")
        logger.info(f"Monitoring queues: {', '.join(queues)}")
        print("\n" + "="*60)
        print(" CONSUMER STARTED (Single-threaded)")
        print(f" Monitoring queues: {', '.join(queues)}")
        print("="*60 + "\n")
        
        try:
            while self.running:
                if max_iterations and iterations >= max_iterations:
                    logger.info(f"Reached maximum iterations ({max_iterations})")
                    break
                
                task_data = self.redis_client.pop_from_queues(queues, timeout=queue_timeout)
                
                if task_data:
                    iterations += 1
                    self.process_single_article(task_data)
                else:
                    if max_iterations is not None:
                        logger.info("No more tasks in queues")
                        print("\nNo more tasks in queues - Consumer stopping\n")
                        break
                    else:
                        logger.debug("No tasks available, waiting...")
                        print("⏳ Waiting for tasks...", end='\r')
                
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.running = False
            self._log_stats()
    
    def _consume_parallel(self, queue_timeout: int = 5, max_iterations: Optional[int] = None):
        """Multi-threaded consumption (default mode)"""
        queues = [
            Config.REDIS_QUEUE_HIGH,
            Config.REDIS_QUEUE_MEDIUM,
            Config.REDIS_QUEUE_LOW
        ]
        
        self.running = True
        iterations = 0
        batch_size = self.max_workers * 2
        
        print("=" * 80)
        print("     MULTI-THREADED CONSUMER STARTED")
        print(f"   Workers: {self.max_workers}")
        print(f"   Batch size: {batch_size}")
        print(f"   Queues: {', '.join(queues)}")
        print("=" * 80 + "\n")
        
        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                while self.running:
                    if max_iterations and iterations >= max_iterations:
                        logger.info(f"Reached maximum iterations ({max_iterations})")
                        break
                    
                    # Fetch batch of tasks
                    tasks = []
                    for _ in range(batch_size):
                        if max_iterations and iterations >= max_iterations:
                            break
                        
                        task_data = self.redis_client.pop_from_queues(queues, timeout=1)
                        if task_data:
                            tasks.append(task_data)
                            iterations += 1
                        else:
                            break
                    
                    if not tasks:
                        if max_iterations is not None:
                            logger.info("No more tasks in queues")
                            print("\nNo more tasks - Consumer stopping\n")
                            break
                        else:
                            print("Waiting for tasks...", end='\r')
                            time.sleep(1)
                            continue
                    
                    # Process batch in parallel
                    print(f"\nProcessing batch of {len(tasks)} articles...")
                    
                    # Update concurrent peak
                    current_workers = min(len(tasks), self.max_workers)
                    with self.stats_lock:
                        if current_workers > self.stats['concurrent_peak']:
                            self.stats['concurrent_peak'] = current_workers
                    
                    # Submit all tasks to thread pool
                    futures = {
                        executor.submit(self.process_single_article, task): task 
                        for task in tasks
                    }
                    
                    # Wait for completion
                    completed = 0
                    for future in as_completed(futures):
                        completed += 1
                        task = futures[future]
                        article_id = task.get('article_id') or task.get('id', 'unknown')
                        
                        try:
                            success = future.result()
                            status = "Success" if success else "Failed"
                            print(f"{status} [{completed}/{len(tasks)}] Article {article_id}")
                        except Exception as e:
                            print(f"[{completed}/{len(tasks)}] Article {article_id} - Error: {e}")
                    
                    # Show progress
                    current_stats = self._get_stats()
                    print(f"\nProgress: {current_stats['processed']} processed "
                          f"({current_stats['successful']} success, {current_stats['failed']} failed, "
                          f"{current_stats['cached']} cached)")
        
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
            print("\n\nConsumer stopped by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            print(f"\n\nConsumer error: {e}")
        finally:
            self.running = False
            self._log_stats()
    
    def consume_batch(self, count: int = None):
        """
        Consume a specific number of articles or all available.
        
        Args:
            count: Number of articles to process (None = all available)
        """
        if count is None:
            queues = [
                Config.REDIS_QUEUE_HIGH,
                Config.REDIS_QUEUE_MEDIUM,
                Config.REDIS_QUEUE_LOW
            ]
            lengths = self.redis_client.get_all_queue_lengths(queues)
            count = sum(lengths.values())
            
            if count == 0:
                logger.info("No tasks available in queues")
                return
        
        logger.info(f"Processing {count} articles in batch mode")
        self.consume(queue_timeout=1, max_iterations=count)
    
    def _log_stats(self):
        """Log processing statistics."""
        stats = self._get_stats()
        
        if self.use_threading:
            print("\n" + "=" * 80)
            print("CONSUMER STATISTICS")
            print("=" * 80)
            print(f"Total processed: {stats['processed']}")
            print(f"Successful: {stats['successful']}")
            print(f"Failed: {stats['failed']}")
            print(f"Duplicates: {stats['duplicates']}")
            print(f"Cached (skipped scraping): {stats['cached']}")
            print(f"Peak concurrent workers: {stats['concurrent_peak']}")
            
            if stats['processed'] > 0:
                success_rate = (stats['successful'] / stats['processed']) * 100
                print(f"Success rate: {success_rate:.1f}%")
            
            print("=" * 80)
        
        logger.info("=" * 50)
        logger.info("Consumer Statistics:")
        logger.info(f"  Total processed: {stats['processed']}")
        logger.info(f"  Successful: {stats['successful']}")
        logger.info(f"  Failed: {stats['failed']}")
        logger.info(f"  Duplicates: {stats['duplicates']}")
        logger.info(f"  Cached: {stats['cached']}")
        if self.use_threading:
            logger.info(f"  Peak workers: {stats['concurrent_peak']}")
        logger.info("=" * 50)
    
    def get_stats(self) -> dict:
        """Get current processing statistics."""
        return self._get_stats()
