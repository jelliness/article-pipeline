"""
Configuration management for the article pipeline.
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class ConfigurationError(Exception):
    """Raised when required configuration is missing."""
    pass


class Config:
    """Configuration class for application settings."""
    
    # Redis Configuration
    REDIS_HOST = os.getenv('REDIS_HOST')
    REDIS_PORT = int(os.getenv('REDIS_PORT')) if os.getenv('REDIS_PORT') else None
    REDIS_DB = int(os.getenv('REDIS_DB')) if os.getenv('REDIS_DB') else None
    
    # MongoDB Configuration
    MONGODB_URI = os.getenv('MONGODB_URI')
    MONGODB_DB = os.getenv('MONGODB_DB')
    
    # Queue Names
    REDIS_QUEUE_HIGH = os.getenv('REDIS_QUEUE_HIGH')
    REDIS_QUEUE_MEDIUM = os.getenv('REDIS_QUEUE_MEDIUM')
    REDIS_QUEUE_LOW = os.getenv('REDIS_QUEUE_LOW')
    
    # Scraping Configuration
    REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT')) if os.getenv('REQUEST_TIMEOUT') else None
    USER_AGENT = os.getenv('USER_AGENT')
    
    # Cache Configuration
    CACHE_ENABLED = os.getenv('CACHE_ENABLED', '').lower() == 'true' if os.getenv('CACHE_ENABLED') else None
    CACHE_TTL = int(os.getenv('CACHE_TTL')) if os.getenv('CACHE_TTL') else None
    CACHE_PREFIX = os.getenv('CACHE_PREFIX')
    
    # Article JSON Path
    ARTICLES_JSON_PATH = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'data',
        'articles.json'
    )
    
    @classmethod
    def validate(cls):
        """
        Validate that all required configuration values are set.
        
        Raises:
            ConfigurationError: If required configuration is missing
        """
        required_vars = {
            'REDIS_HOST': cls.REDIS_HOST,
            'REDIS_PORT': cls.REDIS_PORT,
            'REDIS_DB': cls.REDIS_DB,
            'MONGODB_URI': cls.MONGODB_URI,
            'MONGODB_DB': cls.MONGODB_DB,
            'REDIS_QUEUE_HIGH': cls.REDIS_QUEUE_HIGH,
            'REDIS_QUEUE_MEDIUM': cls.REDIS_QUEUE_MEDIUM,
            'REDIS_QUEUE_LOW': cls.REDIS_QUEUE_LOW,
            'REQUEST_TIMEOUT': cls.REQUEST_TIMEOUT,
            'USER_AGENT': cls.USER_AGENT,
            'CACHE_ENABLED': cls.CACHE_ENABLED,
            'CACHE_TTL': cls.CACHE_TTL,
            'CACHE_PREFIX': cls.CACHE_PREFIX,
        }
        
        missing = [var for var, value in required_vars.items() if value is None]
        
        if missing:
            raise ConfigurationError(
                f"Missing required environment variables: {', '.join(missing)}. "
                f"Please check your .env file."
            )
    
    @classmethod
    def get_queue_by_priority(cls, priority: str) -> str:
        """
        Get queue name by priority level.
        
        Args:
            priority: Priority level (high, medium, low)
            
        Returns:
            Queue name
        """
        priority_map = {
            'high': cls.REDIS_QUEUE_HIGH,
            'medium': cls.REDIS_QUEUE_MEDIUM,
            'low': cls.REDIS_QUEUE_LOW
        }
        return priority_map.get(priority.lower(), cls.REDIS_QUEUE_MEDIUM)
