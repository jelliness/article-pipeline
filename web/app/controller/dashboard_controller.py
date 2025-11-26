"""
Dashboard Controller - Handles dashboard routes and logic
"""
from flask import render_template, jsonify, request
from typing import Dict, Any
import time
from ..model.article_model import ArticleModel
from ..model.pipeline_monitor import PipelineMonitor


class DashboardController:
    """Controller for dashboard operations"""
    
    def __init__(self):
        self.article_model = ArticleModel()
        self.pipeline_monitor = PipelineMonitor()
    
    def index(self):
        """Render main dashboard page"""
        try:
            user_id = 'demo_user'
            
            stats = self.article_model.get_statistics(user_id=user_id)
            recent_articles = self.article_model.get_recent_articles(limit=5, user_id=user_id)
            
            return render_template(
                'dashboard/index.html',
                stats=stats,
                recent_articles=recent_articles
            )
        except Exception as e:
            print(f"Error in dashboard index: {e}")
            return render_template('error.html', error=str(e)), 500
    
    def get_statistics(self):
        """API endpoint for statistics"""
        try:
            user_id = 'demo_user'
            
            stats = self.article_model.get_statistics(user_id=user_id)
            return jsonify({
                "success": True,
                "data": stats
            })
        except Exception as e:
            return jsonify({
                "success": False,
                "error": str(e)
            }), 500
    
    def get_articles(self):
        """API endpoint for articles list"""
        try:
            user_id = 'demo_user'
            
            # Get pagination parameters (if not provided, return all)
            page = request.args.get('page', None, type=int)
            per_page = request.args.get('per_page', None, type=int)
            
            if page and per_page:
                # Calculate skip for pagination
                skip = (page - 1) * per_page
                articles = self.article_model.get_all_articles(limit=per_page, skip=skip, user_id=user_id)
                
                return jsonify({
                    "success": True,
                    "data": articles,
                    "page": page,
                    "per_page": per_page
                })
            else:
                # Return all articles (no limit)
                articles = self.article_model.get_all_articles(limit=0, skip=0, user_id=user_id)
                
                return jsonify({
                    "success": True,
                    "data": articles
                })
        except Exception as e:
            return jsonify({
                "success": False,
                "error": str(e)
            }), 500
    
    def get_article(self, article_id: str):
        """API endpoint for single article"""
        try:
            user_id = 'demo_user'
            
            article = self.article_model.get_article_by_id(article_id, user_id=user_id)
            
            if article:
                return jsonify({
                    "success": True,
                    "data": article
                })
            else:
                return jsonify({
                    "success": False,
                    "error": "Article not found"
                }), 404
        except Exception as e:
            return jsonify({
                "success": False,
                "error": str(e)
            }), 500
    
    def search(self):
        """API endpoint for article search"""
        try:
            user_id = 'demo_user'
            
            query = request.args.get('q', '')
            field = request.args.get('field', None)
            
            if not query:
                return jsonify({
                    "success": False,
                    "error": "Search query is required"
                }), 400
            
            articles = self.article_model.search_articles(query, field, user_id=user_id)
            
            return jsonify({
                "success": True,
                "data": articles,
                "query": query
            })
        except Exception as e:
            return jsonify({
                "success": False,
                "error": str(e)
            }), 500
    
    def add_articles(self):
        """API endpoint to add new articles"""
        try:
            user_id = 'demo_user'
            
            data = request.get_json()
            
            if not data or 'articles' not in data:
                return jsonify({
                    "success": False,
                    "error": "Articles data is required"
                }), 400
            
            articles = data['articles']
            
            if not isinstance(articles, list) or len(articles) == 0:
                return jsonify({
                    "success": False,
                    "error": "Articles must be a non-empty array"
                }), 400
            
            # Validate each article
            for article in articles:
                if not article.get('url'):
                    return jsonify({
                        "success": False,
                        "error": "Each article must have a URL"
                    }), 400
            
            result = self.article_model.add_articles(articles, user_id=user_id)
            
            return jsonify({
                "success": True,
                "data": result
            })
        except Exception as e:
            return jsonify({
                "success": False,
                "error": str(e)
            }), 500
    
    def import_from_file(self):
        """API endpoint to import articles from uploaded JSON file"""
        try:
            user_id = 'demo_user'
            
            # Check if file was uploaded
            if 'file' not in request.files:
                return jsonify({
                    "success": False,
                    "error": "No file uploaded"
                }), 400
            
            file = request.files['file']
            
            # Check if file was selected
            if file.filename == '':
                return jsonify({
                    "success": False,
                    "error": "No file selected"
                }), 400
            
            # Check file extension
            if not file.filename.endswith('.json'):
                return jsonify({
                    "success": False,
                    "error": "File must be a JSON file (.json)"
                }), 400
            
            # Read and parse JSON file
            try:
                import json
                file_content = file.read().decode('utf-8')
                data = json.loads(file_content)
            except json.JSONDecodeError as e:
                return jsonify({
                    "success": False,
                    "error": f"Invalid JSON format: {str(e)}"
                }), 400
            
            # Extract articles array
            articles = data.get('articles', [])
            
            if not isinstance(articles, list) or len(articles) == 0:
                return jsonify({
                    "success": False,
                    "error": "JSON file must contain 'articles' array with at least one article"
                }), 400
            
            # Validate each article
            for idx, article in enumerate(articles):
                if not article.get('url'):
                    return jsonify({
                        "success": False,
                        "error": f"Article at index {idx} is missing 'url' field"
                    }), 400
            
            # Import articles (will be published to Redis queue)
            result = self.article_model.add_articles(articles, user_id=user_id)
            
            return jsonify({
                "success": True,
                "data": {
                    **result,
                    "filename": file.filename,
                    "total_articles_in_file": len(articles)
                }
            })
        except Exception as e:
            return jsonify({
                "success": False,
                "error": str(e)
            }), 500
    
    def get_pipeline_stats(self):
        """API endpoint for comprehensive pipeline statistics"""
        try:
            stats = self.pipeline_monitor.get_comprehensive_stats()
            return jsonify({
                "success": True,
                "data": stats
            })
        except Exception as e:
            return jsonify({
                "success": False,
                "error": str(e)
            }), 500
    
    def get_queue_stats(self):
        """API endpoint for Redis queue statistics"""
        try:
            stats = self.pipeline_monitor.get_queue_stats()
            return jsonify({
                "success": True,
                "data": stats
            })
        except Exception as e:
            return jsonify({
                "success": False,
                "error": str(e)
            }), 500
    
    def get_health(self):
        """API endpoint for pipeline health check"""
        try:
            health = self.pipeline_monitor.get_pipeline_health()
            return jsonify({
                "success": True,
                "data": health
            })
        except Exception as e:
            return jsonify({
                "success": False,
                "error": str(e)
            }), 500
