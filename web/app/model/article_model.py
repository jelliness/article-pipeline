"""
Article Model - Business logic for article data
"""
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
import sys
import os
from .database import Database

# Add src to path for cross-directory imports
src_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'src')
sys.path.insert(0, src_path)

# Import pipeline components
try:
    from publisher.publisher import ArticlePublisher
    from database.redis_client import RedisClient
    from utils.config import Config
    PUBLISHER_AVAILABLE = True
    print(f"[ArticleModel] Publisher components loaded successfully")
except Exception as e:
    PUBLISHER_AVAILABLE = False
    print(f"[ArticleModel] Could not load Publisher: {e}")


class ArticleModel:
    """Article data model with business logic"""
    
    def __init__(self):
        self.db = Database()
        self.collection_name = "articles"
        
        # Initialize Publisher for queue publishing with metadata enrichment
        self.publisher = None
        if PUBLISHER_AVAILABLE:
            try:
                redis_client = RedisClient(
                    host=Config.REDIS_HOST,
                    port=Config.REDIS_PORT,
                    db=Config.REDIS_DB
                )
                self.publisher = ArticlePublisher(redis_client)
                print(f"[ArticleModel] Publisher initialized successfully")
            except Exception as e:
                print(f"[ArticleModel] Could not initialize Publisher: {e}")
                import traceback
                traceback.print_exc()
    
    def get_all_articles(self, limit: int = 100, skip: int = 0, user_id: str = None) -> List[Dict[str, Any]]:
        """Retrieve all articles with pagination and optional user filtering"""
        try:
            collection = self.db.get_collection(self.collection_name)
            
            # Filter by user_id for multi-tenancy
            query = {'user_id': user_id} if user_id else {}
            
            # If limit is 0, return all articles
            if limit == 0:
                articles = list(collection.find(query).sort("created_at", -1))
            else:
                articles = list(collection.find(query).skip(skip).limit(limit).sort("created_at", -1))
            
            # Convert ObjectId to string for JSON serialization
            for article in articles:
                article['_id'] = str(article['_id'])
            
            return articles
        except Exception as e:
            print(f"Error fetching articles: {e}")
            return []
    
    def get_article_by_id(self, article_id: str, user_id: str = None) -> Optional[Dict[str, Any]]:
        """Retrieve a single article by ID with optional user filtering"""
        try:
            collection = self.db.get_collection(self.collection_name)
            
            # Filter by user_id for multi-tenancy
            query = {"article_id": article_id}
            if user_id:
                query["user_id"] = user_id
            
            article = collection.find_one(query)
            
            if article:
                article['_id'] = str(article['_id'])
            
            return article
        except Exception as e:
            print(f"Error fetching article {article_id}: {e}")
            return None
    
    def get_statistics(self, user_id: str = None) -> Dict[str, Any]:
        """Get article statistics with optional user filtering"""
        try:
            collection = self.db.get_collection(self.collection_name)
            
            # Filter by user_id for multi-tenancy
            base_query = {'user_id': user_id} if user_id else {}
            
            total_count = collection.count_documents(base_query)
            
            # Count by status
            status_counts = {}
            for status in ['pending', 'processing', 'completed', 'failed']:
                status_query = {**base_query, "status": status}
                count = collection.count_documents(status_query)
                status_counts[status] = count
            
            # Count by category
            category_pipeline = [
                {"$match": base_query} if base_query else {"$match": {}},
                {"$group": {"_id": "$category", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            categories = list(collection.aggregate(category_pipeline))
            
            # Count by priority
            priority_pipeline = [
                {"$match": base_query} if base_query else {"$match": {}},
                {"$group": {"_id": "$priority", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            priorities = list(collection.aggregate(priority_pipeline))
            
            # Count by source
            source_pipeline = [
                {"$match": base_query} if base_query else {"$match": {}},
                {"$group": {"_id": "$source", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            sources = list(collection.aggregate(source_pipeline))
            
            # Activity timeline - Last 7 days
            from datetime import datetime, timedelta
            activity_timeline = []
            for i in range(6, -1, -1):  # Last 7 days
                date = datetime.now() - timedelta(days=i)
                date_start = date.replace(hour=0, minute=0, second=0, microsecond=0)
                date_end = date.replace(hour=23, minute=59, second=59, microsecond=999999)
                
                # Count articles added (created on this day)
                added_query = {**base_query, "created_at": {"$gte": date_start, "$lte": date_end}}
                added_count = collection.count_documents(added_query)
                
                # Count articles processed (completed on this day)
                processed_query = {
                    **base_query,
                    "status": "completed",
                    "updated_at": {"$gte": date_start, "$lte": date_end}
                }
                processed_count = collection.count_documents(processed_query)
                
                activity_timeline.append({
                    "date": date.strftime("%m/%d"),
                    "added": added_count,
                    "processed": processed_count
                })
            
            return {
                "total": total_count,
                "by_status": status_counts,
                "by_category": [{"name": c["_id"], "count": c["count"]} for c in categories],
                "by_priority": [{"name": p["_id"], "count": p["count"]} for p in priorities],
                "by_source": [{"name": s["_id"], "count": s["count"]} for s in sources[:10]],  # Top 10 sources
                "activity_timeline": activity_timeline
            }
        except Exception as e:
            print(f"Error fetching statistics: {e}")
            return {
                "total": 0,
                "by_status": {},
                "by_category": [],
                "by_priority": [],
                "by_source": [],
                "activity_timeline": []
            }
    
    def get_recent_articles(self, limit: int = 10, user_id: str = None) -> List[Dict[str, Any]]:
        """Get most recent articles with optional user filtering"""
        try:
            collection = self.db.get_collection(self.collection_name)
            
            # Filter by user_id for multi-tenancy
            query = {'user_id': user_id} if user_id else {}
            
            articles = list(collection.find(query).sort("created_at", -1).limit(limit))
            
            for article in articles:
                article['_id'] = str(article['_id'])
            
            return articles
        except Exception as e:
            print(f"Error fetching recent articles: {e}")
            return []
    
    def search_articles(self, query: str, field: Optional[str] = None, user_id: str = None) -> List[Dict[str, Any]]:
        """Search articles by text with optional user filtering"""
        try:
            collection = self.db.get_collection(self.collection_name)
            
            if field:
                # Search in specific field
                search_filter = {field: {"$regex": query, "$options": "i"}}
            else:
                # Search across multiple fields
                search_filter = {
                    "$or": [
                        {"title": {"$regex": query, "$options": "i"}},
                        {"source": {"$regex": query, "$options": "i"}},
                        {"category": {"$regex": query, "$options": "i"}},
                        {"url": {"$regex": query, "$options": "i"}}
                    ]
                }
            
            # Add user_id filter for multi-tenancy
            if user_id:
                search_filter = {"$and": [search_filter, {"user_id": user_id}]}
            
            articles = list(collection.find(search_filter).limit(100))
            
            for article in articles:
                article['_id'] = str(article['_id'])
            
            return articles
        except Exception as e:
            print(f"Error searching articles: {e}")
            return []
    
    def add_articles(self, articles: List[Dict[str, Any]], user_id: str = None) -> Dict[str, Any]:
        """Publish articles to Redis queue via Publisher (does NOT write to MongoDB directly)"""
        try:
            if not self.publisher:
                raise Exception("Publisher not available - cannot queue articles")
            
            if not user_id:
                raise Exception("user_id is required for multi-tenancy")
            
            # Get the highest existing article_id for sequential numbering
            # Find all numeric article_ids and get the maximum
            collection = self.db.get_collection(self.collection_name)
            
            # Get all articles and find the highest numeric ID
            all_articles = collection.find({}, {"article_id": 1})
            max_id = 0
            
            for article in all_articles:
                article_id = article.get('article_id', '0')
                try:
                    
                    numeric_id = int(article_id)
                    if numeric_id > max_id:
                        max_id = numeric_id
                except (ValueError, TypeError):
                    # Skip non-numeric IDs
                    continue
            
            last_id = max_id if max_id > 0 else 0
            
            # Publish to Redis queues via Publisher
            # The consumer will handle scraping and storing in MongoDB
            published_count = 0
            failed_articles = []
            
            print(f"[ArticleModel] Publishing {len(articles)} articles via Publisher...")
            
            for idx, article in enumerate(articles, start=1):
                # Generate sequential ID if not provided
                article_id = article.get('id') or article.get('article_id') or str(last_id + idx).zfill(3)
                
                article_for_publisher = {
                    'id': article_id,
                    'url': article['url'],
                    'source': article.get('source', 'Unknown'),
                    'category': article.get('category', 'other'),
                    'priority': article.get('priority', 'medium'),
                    'user_id': user_id  
                }
                
                try:
                    if self.publisher.publish_single_article(article_for_publisher):
                        published_count += 1
                        print(f"[ArticleModel] Published article {article_for_publisher['id']} via Publisher")
                    else:
                        failed_articles.append(article_for_publisher['id'])
                        print(f"[ArticleModel] Article {article_for_publisher['id']} not published (duplicate or error)")
                except Exception as e:
                    failed_articles.append(article_for_publisher['id'])
                    print(f"[ArticleModel] Error publishing article: {e}")
            
            return {
                "published_to_queue": published_count,
                "failed": len(failed_articles),
                "failed_ids": failed_articles,
                "message": f"Published {published_count} articles to pipeline queue. Consumer will process and store them."
            }
        except Exception as e:
            print(f"Error publishing articles: {e}")
            raise e
