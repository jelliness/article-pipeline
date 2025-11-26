"""
Article Pipeline - Main Entry Point
Consumer service for processing articles from Redis queues.
Articles are submitted via web dashboard/API, not from JSON file.
"""
import argparse
import logging
import sys
from utils.config import Config, ConfigurationError
from database.redis_client import RedisClient
from database.mongodb import ArticleDatabase
from services.scraper import ArticleScraper
from consumer.consumer import ArticleConsumer


def setup_logging(verbose: bool = False):
    """Configure logging for the application."""
    level = logging.DEBUG if verbose else logging.INFO
    
    # Use UTF-8 encoding for file handler to support Unicode characters
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('pipeline.log', encoding='utf-8')
        ]
    )


def run_consumer(
    redis_client: RedisClient,
    database: ArticleDatabase,
    scraper: ArticleScraper,
    batch_mode: bool = False,
    count: int = None,
    use_threading: bool = True,
    workers: int = 10
):
    """
    Run the consumer to process articles from Redis queues.
    
    Args:
        redis_client: RedisClient instance
        database: ArticleDatabase instance
        scraper: ArticleScraper instance
        batch_mode: If True, process all available tasks and exit
        count: Number of articles to process (batch mode only)
        use_threading: If True, use multi-threaded processing (default)
        workers: Number of concurrent workers (multi-threaded mode only)
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting Consumer...")
    
    try:
        consumer = ArticleConsumer(
            redis_client, 
            database, 
            scraper, 
            max_workers=workers,
            use_threading=use_threading
        )
        
        if batch_mode:
            consumer.consume_batch(count)
        else:
            consumer.consume()
        
        return consumer.get_stats()
    except Exception as e:
        logger.error(f"Consumer error: {e}")
        return None


def main():
    """Main entry point for the consumer service."""
    parser = argparse.ArgumentParser(
        description='Article Pipeline Consumer - Processes articles from Redis queues'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging (DEBUG level)'
    )
    
    parser.add_argument(
        '--batch',
        action='store_true',
        help='Run in batch mode (process all queued articles and exit)'
    )
    
    parser.add_argument(
        '--count',
        type=int,
        default=None,
        help='Number of articles to process in batch mode'
    )
    
    parser.add_argument(
        '--single-threaded',
        action='store_true',
        help='Use single-threaded consumer (legacy mode, slower)'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=10,
        help='Number of concurrent workers (default: 10, only used in multi-threaded mode)'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("Article Pipeline Consumer Service")
    logger.info("=" * 60)
    logger.info("NOTE: Articles are submitted via web dashboard, not auto-published")
    logger.info("=" * 60)
    
    # Validate configuration
    try:
        Config.validate()
        logger.info("Configuration validated successfully")
    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    
    # Initialize components
    try:
        logger.info("Initializing components...")
        
        # Redis client
        redis_client = RedisClient(
            host=Config.REDIS_HOST,
            port=Config.REDIS_PORT,
            db=Config.REDIS_DB
        )
        
        # MongoDB database
        database = ArticleDatabase(
            mongodb_uri=Config.MONGODB_URI,
            db_name=Config.MONGODB_DB
        )
        
        # Article scraper
        scraper = ArticleScraper(
            user_agent=Config.USER_AGENT,
            timeout=Config.REQUEST_TIMEOUT
        )
        
        logger.info("Components initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize components: {e}")
        logger.error("Please ensure Redis and MongoDB are running")
        sys.exit(1)
    
    # Execute consumer service
    try:
        logger.info("Starting consumer service...")
        logger.info("Waiting for articles submitted via web dashboard...")
        
        # Multi-threaded is default, single-threaded is legacy mode
        use_threading = not args.single_threaded
        
        if use_threading:
            logger.info(f"Multi-threaded mode: {args.workers} workers (5-10x faster)")
        else:
            logger.info("Single-threaded mode (legacy, slower)")
        
        if args.batch:
            logger.info(f"Running in batch mode (count={args.count or 'all'})")
        else:
            logger.info("Running in continuous mode (Press Ctrl+C to stop)")
        
        run_consumer(
            redis_client,
            database,
            scraper,
            batch_mode=args.batch,
            count=args.count,
            use_threading=use_threading,
            workers=args.workers
        )
        
    except KeyboardInterrupt:
        logger.info("\nShutdown requested by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        # Cleanup
        logger.info("Cleaning up...")
        redis_client.close()
        database.close()
        logger.info("Consumer service shutdown complete")


if __name__ == "__main__":
    main()