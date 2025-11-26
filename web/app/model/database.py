"""
Database Model - Singleton pattern for MongoDB connection
"""
from pymongo import MongoClient
from typing import Optional, List, Dict, Any
import threading


class Database:
    """Database connection manager"""
    _instance: Optional['Database'] = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._client: Optional[MongoClient] = None
            self._db = None
            self._initialized = True
    
    def connect(self, connection_string: str = "mongodb://localhost:27017/", 
                database_name: str = "article_pipeline") -> None:
        """Establish database connection with connection pooling"""
        if self._client is None:
            self._client = MongoClient(
                connection_string,
                maxPoolSize=50,
                minPoolSize=10,
                serverSelectionTimeoutMS=5000
            )
            self._db = self._client[database_name]
            print(f"[Database] Connected to MongoDB database: {database_name}")
    
    def get_collection(self, collection_name: str):
        """Get a collection from the database"""
        if self._db is None:
            raise ConnectionError("Database not connected. Call connect() first.")
        return self._db[collection_name]
    
    def close(self) -> None:
        """Close database connection"""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
    
    @property
    def is_connected(self) -> bool:
        """Check if database is connected"""
        return self._client is not None and self._db is not None
