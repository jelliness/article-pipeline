"""
Dashboard Application Entry Point
"""
import os

from app import create_app


def main():
    """Run the dashboard application"""
    # Create Flask app using singleton factory
    app = create_app()
    
    # Configuration
    host = os.getenv('DASHBOARD_HOST', '0.0.0.0')
    port = int(os.getenv('DASHBOARD_PORT', 5000))
    debug = os.getenv('DASHBOARD_DEBUG', 'True').lower() == 'true'
    
    
    # Run the application
    app.run(
        host=host,
        port=port,
        debug=debug,
        use_reloader=debug
    )


if __name__ == '__main__':
    main()
