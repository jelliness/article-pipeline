"""
Database module for MongoDB operations.
Handles article storage with duplicate detection and schema validation.
"""
import logging
from typing import Optional, Dict, Any
from datetime import datetime
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError, PyMongoError
from utils.base import Base

logger = logging.getLogger(__name__)


class ArticleDatabase(metaclass=Base):
    """MongoDB handler for article storage. Shared across the application."""
    
    _initialized = False
    
    def __init__(self, mongodb_uri: str, db_name: str):
        """
        Initialize MongoDB connection (reuses existing instance if already created).
        
        Args:
            mongodb_uri: MongoDB connection URI
            db_name: Database name
        """
        # Prevent re-initialization
        if self._initialized:
            logger.debug("MongoDB client already initialized")
            return
        
        try:
            self.client = MongoClient(
                mongodb_uri,
                maxPoolSize=50,
                minPoolSize=10,
                serverSelectionTimeoutMS=5000
            )
            self.db = self.client[db_name]
            self.articles = self.db['articles']
            self._setup_indexes()
            self._initialized = True
            logger.info(f"Connected to MongoDB database: {db_name}")
        except PyMongoError as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def _setup_indexes(self):
        """Create indexes for efficient querying and duplicate prevention."""
        try:
            # === Unique Indexes for Duplicate Prevention ===
            self.articles.create_index([('article_id', ASCENDING)], unique=True, name='idx_article_id_unique')
            self.articles.create_index([('url', ASCENDING)], unique=True, name='idx_url_unique')
            self.articles.create_index([('user_id', ASCENDING)], name='idx_user_id')
            
            # === Compound Indexes for Dashboard Queries ===
            # User + Status (for status filtering and counts)
            self.articles.create_index(
                [('user_id', ASCENDING), ('status', ASCENDING)], 
                name='idx_user_status'
            )
            
            # User + Created_at (for recent articles, descending order)
            self.articles.create_index(
                [('user_id', ASCENDING), ('created_at', -1)], 
                name='idx_user_created_desc'
            )
            
            # User + Category (for category aggregation)
            self.articles.create_index(
                [('user_id', ASCENDING), ('category', ASCENDING)], 
                name='idx_user_category'
            )
            
            # User + Priority (for priority aggregation)
            self.articles.create_index(
                [('user_id', ASCENDING), ('priority', ASCENDING)], 
                name='idx_user_priority'
            )
            
            # User + Source (for source aggregation)
            self.articles.create_index(
                [('user_id', ASCENDING), ('source', ASCENDING)], 
                name='idx_user_source'
            )
            
            # User + Status + Updated_at (for activity timeline processed articles)
            self.articles.create_index(
                [('user_id', ASCENDING), ('status', ASCENDING), ('updated_at', -1)], 
                name='idx_user_status_updated'
            )
            
            # === Single Field Indexes for Filtering ===
            self.articles.create_index([('status', ASCENDING)], name='idx_status')
            self.articles.create_index([('priority', ASCENDING)], name='idx_priority')
            self.articles.create_index([('category', ASCENDING)], name='idx_category')
            self.articles.create_index([('source', ASCENDING)], name='idx_source')
            
            # === Time-based Indexes ===
            self.articles.create_index([('created_at', -1)], name='idx_created_desc')
            self.articles.create_index([('updated_at', -1)], name='idx_updated_desc')
            
            # === URL Hash Index for Cache Lookups ===
            self.articles.create_index([('url_hash', ASCENDING)], name='idx_url_hash', sparse=True)
            
            logger.info("✅ Database indexes created successfully")
            logger.info("   - 2 unique indexes (article_id, url)")
            logger.info("   - 6 compound indexes (user + field combinations)")
            logger.info("   - 6 single field indexes (status, priority, category, source, times, url_hash)")
        except PyMongoError as e:
            logger.warning(f"Index creation warning: {e}")
    
    def insert_article(self, article_data: Dict[str, Any]) -> bool:
        """
        Insert a new article into the database.
        
        Args:
            article_data: Dictionary containing article information
            
        Returns:
            bool: True if inserted successfully, False if duplicate
        """
        try:
            # Add timestamp
            article_data['created_at'] = datetime.utcnow()
            article_data['updated_at'] = datetime.utcnow()
            
            # Validate required fields
            required_fields = ['article_id', 'url', 'source', 'category', 'priority']
            if not all(field in article_data for field in required_fields):
                logger.error(f"Missing required fields in article data: {article_data}")
                return False
            
            self.articles.insert_one(article_data)
            logger.info(f"Inserted article: {article_data['article_id']} - {article_data.get('title', 'N/A')}")
            return True
            
        except DuplicateKeyError:
            logger.warning(f"Duplicate article detected: {article_data.get('article_id', 'unknown')}")
            return False
        except PyMongoError as e:
            logger.error(f"Database error while inserting article: {e}")
            return False
    
    def update_article(self, article_id: str, update_data: Dict[str, Any]) -> bool:
        """
        Update an existing article.
        
        Args:
            article_id: Unique article identifier
            update_data: Dictionary containing fields to update
            
        Returns:
            bool: True if updated successfully
        """
        try:
            update_data['updated_at'] = datetime.utcnow()
            result = self.articles.update_one(
                {'article_id': article_id},
                {'$set': update_data}
            )
            
            if result.modified_count > 0:
                logger.info(f"Updated article: {article_id}")
                return True
            else:
                logger.warning(f"No article found to update: {article_id}")
                return False
                
        except PyMongoError as e:
            logger.error(f"Database error while updating article: {e}")
            return False
    
    def get_article(self, article_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve an article by ID.
        
        Args:
            article_id: Unique article identifier
            
        Returns:
            Article document or None if not found
        """
        try:
            return self.articles.find_one({'article_id': article_id})
        except PyMongoError as e:
            logger.error(f"Database error while retrieving article: {e}")
            return None
    
    def get_all_articles(self, limit: int = 100) -> list:
        """
        Retrieve all articles with optional limit.
        
        Args:
            limit: Maximum number of articles to retrieve
            
        Returns:
            List of article documents
        """
        try:
            return list(self.articles.find().limit(limit))
        except PyMongoError as e:
            logger.error(f"Database error while retrieving articles: {e}")
            return []
    
    def close(self):
        """Close the MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
