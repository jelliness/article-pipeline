"""
Flask Application Factory with Singleton Pattern
"""
from flask import Flask
import threading
from typing import Optional
import os
from pathlib import Path
from dotenv import load_dotenv
from .model.database import Database

# Load environment variables from root .env file
env_path = Path(__file__).parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)


class FlaskApp:
    """Singleton Flask application factory"""
    _instance: Optional['FlaskApp'] = None
    _lock = threading.Lock()
    _app: Optional[Flask] = None
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def create_app(self, config: Optional[dict] = None) -> Flask:
        """Create and configure the Flask application"""
        if self._app is not None:
            return self._app
        
        self._app = Flask(
            __name__,
            template_folder='view/templates',
            static_folder='view/static'
        )
        
        # Default configuration from environment variables
        self._app.config.update(
            SECRET_KEY=os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production'),
            JSON_SORT_KEYS=False,
            MONGODB_URI=os.getenv('MONGODB_URI', 'mongodb://localhost:27017/'),
            DATABASE_NAME=os.getenv('DATABASE_NAME', 'article_pipeline')
        )
        
        # Update with custom config if provided
        if config:
            self._app.config.update(config)
        
        # Initialize database
        self._init_database()
        
        # Register blueprints/routes
        self._register_routes()
        
        return self._app
    
    def _init_database(self):
        """Initialize database connection"""
        db = Database()
        db.connect(
            connection_string=self._app.config['MONGODB_URI'],
            database_name=self._app.config['DATABASE_NAME']
        )
    
    def _register_routes(self):
        """Register application routes"""
        from .controller.dashboard_controller import DashboardController
        
        dashboard = DashboardController()
        
        # Dashboard routes
        self._app.add_url_rule('/', 'index', dashboard.index)
        self._app.add_url_rule('/dashboard', 'dashboard', dashboard.index)
        
        # API routes
        self._app.add_url_rule('/api/statistics', 'api_statistics', dashboard.get_statistics)
        self._app.add_url_rule('/api/articles', 'api_articles', dashboard.get_articles)
        self._app.add_url_rule('/api/articles/<article_id>', 'api_article', dashboard.get_article)
        self._app.add_url_rule('/api/articles/add', 'api_articles_add', dashboard.add_articles, methods=['POST'])
        self._app.add_url_rule('/api/articles/import', 'api_articles_import', dashboard.import_from_file, methods=['POST'])
        self._app.add_url_rule('/api/search', 'api_search', dashboard.search)
        
        # Pipeline monitoring routes
        self._app.add_url_rule('/api/pipeline/stats', 'api_pipeline_stats', dashboard.get_pipeline_stats)
        self._app.add_url_rule('/api/pipeline/queue', 'api_queue_stats', dashboard.get_queue_stats)
        self._app.add_url_rule('/api/pipeline/health', 'api_health', dashboard.get_health)
    
    def get_app(self) -> Optional[Flask]:
        """Get the Flask application instance"""
        return self._app


def create_app(config: Optional[dict] = None) -> Flask:
    """Factory function to create Flask app"""
    app_factory = FlaskApp()
    return app_factory.create_app(config)
