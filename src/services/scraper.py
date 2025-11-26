"""
Web scraping service for extracting article content.
"""
import logging
import cloudscraper
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any
from requests.exceptions import RequestException, Timeout, ConnectionError
from utils.base import Base

logger = logging.getLogger(__name__)


class ArticleScraper(metaclass=Base):
    """Service for scraping article content from URLs. Shared across the application."""
    
    _initialized = False
    
    def __init__(self, user_agent: str, timeout: int = 10):
        """
        Initialize the scraper (reuses existing instance if already created).
        
        Args:
            user_agent: User agent string for requests
            timeout: Request timeout in seconds
        """
        # Prevent re-initialization
        if self._initialized:
            logger.debug("Article scraper already initialized")
            return
        
        self.user_agent = user_agent
        self.timeout = timeout

        # Create cloudscraper instance (handles Cloudflare and bot protection)
        # This session persists cookies and maintains state across requests
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            },
            delay=10
        )
        self.headers = {
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
        }
        self._initialized = True
        logger.info("Article scraper initialized with cloudscraper")
    
    def scrape_article(self, url: str, max_retries: int = 3) -> Dict[str, Any]:
        """
        Scrape article content from a URL with retry logic.
        
        Args:
            url: Article URL
            max_retries: Maximum number of retry attempts
            
        Returns:
            Dictionary containing scraped data and status
        """
        result = {
            'url': url,
            'title': None,
            'content': None,
            'status': 'unknown',
            'error': None
        }
        
        import time
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    wait_time = 2 ** attempt  # Exponential backoff: 2s, 4s, 8s
                    logger.info(f"Retry attempt {attempt + 1}/{max_retries} for {url} after {wait_time}s delay")
                    time.sleep(wait_time)
                
                logger.info(f"Scraping article: {url}")
                
                # Create a fresh scraper instance for each request to avoid cookie/session issues
                # Reusing the same scraper session can sometimes cause issues with certain sites
                fresh_scraper = cloudscraper.create_scraper(
                    browser={
                        'browser': 'chrome',
                        'platform': 'windows',
                        'mobile': False
                    },
                    delay=10
                )
                
                # Make HTTP request using cloudscraper (bypasses Cloudflare protection)
                response = fresh_scraper.get(
                    url,
                    headers=self.headers,
                    timeout=self.timeout,
                    allow_redirects=True
                )
                
                # Check if request was successful
                if response.status_code == 403 and attempt < max_retries - 1:
                    logger.warning(f"HTTP 403 on attempt {attempt + 1}, retrying...")
                    continue
                    
                if response.status_code != 200:
                    result['status'] = 'failed'
                    result['error'] = f"HTTP {response.status_code}"
                    logger.warning(f"Failed to fetch {url}: HTTP {response.status_code}")
                    return result
                
                # Parse HTML with proper decoding
                try:
                    # Try to detect encoding from content if needed
                    if response.encoding is None or response.encoding == 'ISO-8859-1':
                        # Use apparent_encoding or default to utf-8
                        from charset_normalizer import from_bytes
                        detected = from_bytes(response.content).best()
                        encoding = detected.encoding if detected else 'utf-8'
                    else:
                        encoding = response.encoding
                    
                    # Decode the content
                    html_text = response.content.decode(encoding, errors='replace')
                    
                except Exception as e:
                    logger.warning(f"Encoding detection failed: {e}, using response.text")
                    html_text = response.text
                
                # Parse HTML
                soup = BeautifulSoup(html_text, 'html.parser')
                
                # Extract title
                title = self._extract_title(soup)
                result['title'] = title
                
                # Extract content (basic extraction)
                content = self._extract_content(soup)
                result['content'] = content
                
                # Determine status
                if title:
                    result['status'] = 'success'
                    logger.info(f"Successfully scraped: {title[:50]}...")
                else:
                    result['status'] = 'partial'
                    result['error'] = 'No title found'
                    logger.warning(f"Partial scrape for {url}: No title found")
                
                return result
                
            except Timeout:
                if attempt < max_retries - 1:
                    logger.warning(f"Timeout on attempt {attempt + 1}, retrying...")
                    continue
                result['status'] = 'failed'
                result['error'] = 'Request timeout'
                logger.error(f"Timeout while scraping {url}")
                return result
                
            except ConnectionError:
                if attempt < max_retries - 1:
                    logger.warning(f"Connection error on attempt {attempt + 1}, retrying...")
                    continue
                result['status'] = 'failed'
                result['error'] = 'Connection error'
                logger.error(f"Connection error while scraping {url}")
                return result
                
            except RequestException as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Request error on attempt {attempt + 1}, retrying...")
                    continue
                result['status'] = 'failed'
                result['error'] = f'Request error: {str(e)}'
                logger.error(f"Request error while scraping {url}: {e}")
                return result
                
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Unexpected error on attempt {attempt + 1}, retrying...")
                    continue
                result['status'] = 'failed'
                result['error'] = f'Unexpected error: {str(e)}'
                logger.error(f"Unexpected error while scraping {url}: {e}")
                return result
        
        # Should not reach here, but return failed result as fallback
        result['status'] = 'failed'
        result['error'] = 'Max retries exceeded'
        return result
    
    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extract article title from HTML.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Article title or None
        """

        meta_selectors = [
            ('meta', {'property': 'og:title'}),
            ('meta', {'name': 'twitter:title'}),
            ('meta', {'name': 'title'}),
        ]
        
        for tag, attrs in meta_selectors:
            element = soup.find(tag, attrs)
            if element:
                title = element.get('content', '').strip()
                if title:
                    logger.info(f"Extracted title from meta tag: {title[:50]}...")
                    return title
        
        # Try all h1 tags first (most common for article titles)
        # This catches h1 tags whether they're in header tags or not
        all_h1s = soup.find_all('h1', limit=10)  
        
        # Priority 1: h1 with common article title classes
        title_classes = [
            'entry-title', 'article-title', 'post-title', 'title',
            'headline', 'article-headline', 'post-headline',
            'page-title', 'single-title', 'story-title', 'td-post-title'
        ]
        
        for h1 in all_h1s:
            classes = h1.get('class', [])
            if classes:
                class_str = ' '.join(classes).lower()
                for title_class in title_classes:
                    if title_class in class_str:
                        title = h1.get_text(separator=' ', strip=True)
                        if title and len(title) > 5:  # Ensure it's not just whitespace or too short
                            return title
        
        # Priority 2: h1 inside header tag (regardless of classes)
        headers = soup.find_all('header', limit=5)
        for header in headers:
            h1 = header.find('h1')
            if h1:
                title = h1.get_text(separator=' ', strip=True)
                if title and len(title) > 5:
                    return title
        
        # Priority 3: h1 inside article tag
        article = soup.find('article')
        if article:
            h1 = article.find('h1')
            if h1:
                title = h1.get_text(separator=' ', strip=True)
                if title and len(title) > 5:
                    return title
        
        # Priority 4: Any h1 inside main content divs
        content_selectors = ['entry-header', 'article-header', 'post-header', 'content-header']
        for selector in content_selectors:
            div = soup.find('div', class_=selector)
            if div:
                h1 = div.find('h1')
                if h1:
                    title = h1.get_text(separator=' ', strip=True)
                    if title and len(title) > 5:
                        return title
        
        # Priority 5: First h1 tag found anywhere 
        if all_h1s:
            for h1 in all_h1s:
                title = h1.get_text(separator=' ', strip=True)
                if title and len(title) > 5:
                    # Skip if it looks like navigation or menu text
                    title_lower = title.lower()
                    skip_keywords = ['menu', 'navigation', 'skip to', 'search', 'logo']
                    if not any(keyword in title_lower for keyword in skip_keywords):
                        return title
        
        # Fallback to title tag (last resort)
        title_tag = soup.find('title')
        if title_tag:
            title = title_tag.get_text(strip=True)
            if title and len(title) > 5:
                # Clean up common title tag patterns like "Title | Site Name"
                # Try to extract just the article title part
                separators = [' | ', ' - ', ' :: ', ' — ']
                for sep in separators:
                    if sep in title:
                        parts = title.split(sep)
                        # Return the longest part (likely the actual title)
                        title = max(parts, key=len).strip()
                        break
                return title
        
        return None
    
    def _extract_content(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extract article content from HTML.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Article content or None
        """
        # Try article tag first
        article = soup.find('article')
        if article:
            content = article.get_text(separator=' ', strip=True)
            if content and len(content) > 100:
                return content[:5000]
        
        # Try specific content classes
        content_classes = [
            'entry-content', 'article-content', 'post-content', 'content',
            'article-body', 'post-body', 'story-content', 'story-body',
            'main-content', 'page-content', 'single-content'
        ]
        
        for class_name in content_classes:
            div = soup.find('div', class_=class_name)
            if div:
                content = div.get_text(separator=' ', strip=True)
                if content and len(content) > 100:
                    return content[:5000]
        
        # Try divs with class containing content keywords
        all_divs = soup.find_all('div')
        for div in all_divs:
            classes = div.get('class', [])
            if classes:
                class_str = ' '.join(classes).lower()
                if any(keyword in class_str for keyword in ['content', 'article', 'post', 'body', 'story']):
                    content = div.get_text(separator=' ', strip=True)
                    if content and len(content) > 100:
                        return content[:5000]
        
        # Try content by ID
        content_ids = ['content', 'article', 'main', 'post']
        for id_name in content_ids:
            element = soup.find(id=id_name)
            if element:
                content = element.get_text(separator=' ', strip=True)
                if content and len(content) > 100:
                    return content[:5000]
        
        # Try main tag
        main = soup.find('main')
        if main:
            content = main.get_text(separator=' ', strip=True)
            if content and len(content) > 100:
                return content[:5000]
        
        # Fallback: get all paragraphs
        paragraphs = soup.find_all('p')
        if paragraphs:
            content = ' '.join([p.get_text(strip=True) for p in paragraphs[:15]])
            if content and len(content) > 100:
                return content[:5000]
        
        return None
