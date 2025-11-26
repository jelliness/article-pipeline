"""
URL normalization and hashing utilities.
"""
import hashlib
from urllib.parse import urlparse, urlunparse
import logging

logger = logging.getLogger(__name__)


class URLNormalizer:
    """Handles URL normalization and cleaning."""
    
    @staticmethod
    def normalize_url(url: str) -> str:
        """
        Normalize URL by:
        - Converting http to https
        - Removing fragments
        - Lowercasing domain
        - Removing trailing slashes
        
        Args:
            url: Original URL
            
        Returns:
            Normalized URL
        """
        try:
            parsed = urlparse(url)
            
            # Convert http to https
            scheme = 'https' if parsed.scheme == 'http' else parsed.scheme
            
            # Lowercase domain
            netloc = parsed.netloc.lower()
            
            # Remove trailing slash from path
            path = parsed.path.rstrip('/')
            if not path:
                path = '/'
            
            # Keep query string as-is, just remove fragment
            normalized = urlunparse((
                scheme,
                netloc,
                path,
                parsed.params,
                parsed.query,
                ''  # Remove fragment
            ))
            
            logger.debug(f"Normalized URL: {url} → {normalized}")
            return normalized
            
        except Exception as e:
            logger.warning(f"Failed to normalize URL {url}: {e}")
            return url
    
    @staticmethod
    def generate_url_hash(url: str) -> str:
        """
        Generate a hash for URL deduplication.
        Uses normalized URL for consistent hashing.
        
        Args:
            url: URL to hash
            
        Returns:
            SHA-256 hash of normalized URL
        """
        normalized = URLNormalizer.normalize_url(url)
        return hashlib.sha256(normalized.encode('utf-8')).hexdigest()
