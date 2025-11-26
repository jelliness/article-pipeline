"""
Article metadata enrichment utilities.
"""
from urllib.parse import urlparse
from datetime import datetime
from typing import Dict, Any, Set
import logging
from .url_normalizer import URLNormalizer

logger = logging.getLogger(__name__)


class MetadataEnricher:
    """Enriches article metadata with additional information."""
    
    @staticmethod
    def enrich_metadata(article: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich article metadata with normalized URL and hash for deduplication.
        
        Args:
            article: Original article data
            
        Returns:
            Enriched article data with additional metadata
        """
        url = article.get('url', '')
        
        # Normalize URL
        normalized_url = URLNormalizer.normalize_url(url)
        
        # Generate URL hash for deduplication
        url_hash = URLNormalizer.generate_url_hash(url)
        
        # Extract domain for display
        parsed = urlparse(normalized_url)
        domain = parsed.netloc.lower()
        
        # Add timestamp
        published_at = datetime.utcnow().isoformat()
        
        # Create enriched metadata
        enriched = article.copy()
        enriched.update({
            'url': normalized_url,  # Replace with normalized URL
            'url_original': url,    # Keep original for reference
            'url_hash': url_hash,   # For deduplication
            'domain': domain,
            'published_at': published_at,
            'enriched': True
        })
        
        logger.debug(f"Enriched metadata for {article.get('id')}: {normalized_url}")
        return enriched
    
    @staticmethod
    def is_duplicate(url_hash: str, seen_hashes: Set[str]) -> bool:
        """
        Check if URL hash already exists (duplicate detection).
        
        Args:
            url_hash: URL hash to check
            seen_hashes: Set of previously seen hashes
            
        Returns:
            True if duplicate
        """
        return url_hash in seen_hashes
