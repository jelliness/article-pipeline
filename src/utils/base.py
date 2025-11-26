"""
Base classes and metaclasses for the application.
"""
import threading


class Base(type):
    """
    Ensures only one instance of a class exists.
    Thread-safe implementation for shared resources.
    """
    _instances = {}
    _lock = threading.Lock()
    
    def __call__(cls, *args, **kwargs):
        """
        Returns existing instance or creates new one if none exists.
        """
        # Thread-safe check and create
        if cls not in cls._instances:
            with cls._lock:
                if cls not in cls._instances:
                    instance = super().__call__(*args, **kwargs)
                    cls._instances[cls] = instance
        return cls._instances[cls]
    
    @classmethod
    def clear_instances(cls):
        """Clear all instances (useful for testing)."""
        cls._instances.clear()
