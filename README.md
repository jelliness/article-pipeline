# Article Pipeline

Publisher–consumer pipeline for web article scraping with Redis, MongoDB, and a real-time dashboard.

## Features

- ✅ **Publisher**: Parses JSON and pushes article tasks to Redis queues
- ✅ **Consumer**: Pops tasks from Redis, scrapes articles, stores in MongoDB
- ✅ **Real-time Dashboard**: Monitor pipeline status, view articles, and submit new tasks
- ✅ **Smart Caching**: Redis-based caching to avoid duplicate scraping
- ✅ **URL Normalization**: URL deduplication
- ✅ **Error Handling**: Retry logic and error tracking and logging
- ✅ **Priority Queues**: High/medium/low priority task processing
- ✅ **Multi-threaded Processing**: Concurrent article scraping (10 workers by default)
- ✅ **Database Indexing**: Optimized MongoDB indexes for fast queries

## Tech Stack

- **Queue**: Redis
- **Database**: MongoDB
- **Web Framework**: Flask
- **Scraper**: BeautifulSoup4 + Cloudscraper

## Quick Start

### Prerequisites

- Python 3.8+
- Redis server running on `localhost:6379`
- MongoDB server running on `localhost:27017`

### Installation

```bash
# Clone repository
git clone https://github.com/jelliness/article-pipeline.git
cd article-pipeline

# Install dependencies
pip install -r requirements.txt

# Configure environment (optional)
cp .env
# Edit .env with your settings
```
## Configuration

Edit `.env` file or use environment variables:

```ini
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
MONGO_URI=mongodb://localhost:27017
MONGO_DB=article_pipeline
```

### How to Run?

**1. Start the Consumer**
```bash
cd src
python main.py
```

**2. Start the Dashboard (in new terminal)**
```bash
cd web
python run.py
```

**3. Access Dashboard**
Open `http://localhost:5000` in your browser



## Project Structure

```
article-pipeline/
├── data/
│   └── articles.json         # Sample article URLs
├── src/
│   ├── main.py              # Consumer entry point
│   ├── publisher/           # Publisher logic
│   ├── consumer/            # Consumer logic
│   ├── services/            # Scraper service
│   ├── database/            # Redis & MongoDB clients
│   └── utils/               # Config, caching, data enrichment
└── web/
    ├── run.py               # Dashboard entry point
    └── app/
        ├── controller/      # Flask routes
        ├── model/           # Data models
        └── view/            # Templates & static files
```

## Core Components

### Publisher
- Reads `articles.json` with article metadata (id, url, source, category, priority)
- Pushes tasks to Redis queues based on priority
- Available via dashboard UI

### Consumer
- Polls Redis queues (high → medium → low priority)
- Scrapes article title and content using BeautifulSoup4
- Stores results in MongoDB with metadata
- Handles duplicates via URL normalization

### Database Schema (MongoDB)

Example Output:

```json
{
  "_id": {
    "$oid": "69265a5262bfe68e8ea3074d"
  },
  "article_id": "005",
  "url": "https://manilastandard.net/news/crime-courts/314631070/sulus-2nd-most-wanted-fugitive.html",
  "url_original": "https://manilastandard.net/news/crime-courts/314631070/sulus-2nd-most-wanted-fugitive.html",
  "url_hash": "4ef5ecf60d6d8000e1149891379ebbe4e830a168efeea076b24b65dd4b6c1077",
  "domain": "manilastandard.net",
  "source": "Manila Standard",
  "category": "crime",
  "priority": "high",
  "status": "completed",
  "title": "Sulu's 2nd most wanted fugitive dies in clash with troops - Manila Standard",
  "content": "Crime/Courts Last updated August 17, 2025...",
  "scrape_status": "success",
  "scrape_error": null,
  "scraped_at": {
    "$date": "2025-11-26T01:39:30.344Z"
  },
  "published_at": "2025-11-26T01:39:26.146604",
  "enriched": true,
  "user_id": "demo_user",
  "created_at": {
    "$date": "2025-11-26T01:39:30.345Z"
  },
  "updated_at": {
    "$date": "2025-11-26T01:39:30.345Z"
  }
}
```

**Schema Fields:**
- `_id` - MongoDB ObjectId (auto-generated)
- `article_id` - Unique article identifier from source
- `url` - Normalized article URL
- `url_original` - Original URL as submitted
- `url_hash` - SHA-256 hash for cache lookups
- `domain` - Extracted domain name
- `source`, `category`, `priority` - Article metadata
- `status` - Processing status (completed/failed)
- `title`, `content` - Scraped article data
- `scrape_status`, `scrape_error` - Scraping result details
- `scraped_at` - Timestamp when article was scraped
- `published_at` - Article publication date (if available)
- `enriched` - Whether content enrichment was applied
- `user_id` - User who submitted the article
- `created_at`, `updated_at` - Record timestamps

## Additional Features 

### 1. **Real-time Dashboard**
- Live pipeline monitoring
- Article submission interface
- Status tracking per article
- Statistics and metrics

### 2. **Smart Caching Layer**
- Redis-based content caching
- Automatic cache invalidation
- Reduces redundant scraping

### 3. **URL Normalization**
- Removes duplicates (e.g., `http://` vs `https://`)
- Query parameter handling
- Trailing slash normalization

### 4. **Multi-threaded Consumer**
- Concurrent processing with ThreadPoolExecutor
- Configurable worker pool (default: 10 workers)
- Thread-safe operations
- Graceful shutdown handling

### 5. **Database Indexing Strategy**

**Unique Indexes (Duplicate Prevention):**
- `article_id` - Prevents duplicate article IDs
- `url` - Prevents duplicate URLs

**Compound Indexes (Dashboard Performance):**
- `user_id + status` - Status filtering and counts
- `user_id + created_at` - Recent articles (sorted)
- `user_id + category` - Category aggregation
- `user_id + priority` - Priority aggregation
- `user_id + source` - Source aggregation
- `user_id + status + updated_at` - Activity timeline

**Single Field Indexes:**
- `status`, `priority`, `category`, `source` - Fast filtering
- `created_at`, `updated_at` - Time-based queries
- `url_hash` - Cache lookups (sparse index)

**Total: 14 indexes** ensuring sub-millisecond query performance.

## Requirements Checklist

- [x] Publisher parses JSON and pushes to Redis
- [x] Consumer pops from Redis and scrapes articles
- [x] MongoDB database with proper schema
- [x] Duplicate handling via URL normalization
- [x] End-to-end pipeline integration
- [x] Error handling (failed requests, invalid HTML, unreachable URLs)
- [x] Additional innovative features:
  - [x] Real-time dashboard with live monitoring
  - [x] Multi-threaded consumer (10 concurrent workers)
  - [x] Smart caching layer (Redis-based)
  - [x] Database indexing strategy 
  - [x] URL normalization and deduplication
- [x] Documentation with setup instructions

## Error Handling

The pipeline can handle problems safely and keep running. 

- If the internet connection fails, it will try again, waiting a bit longer each time.
- If the HTML of a page is broken or messy, it will still try to get the basic info.
- If a link cannot be reached at all, it will record it as failed in the database.
- If the same link appears more than once, it will recognize the duplicate and skip it.

## Sample Usage

### Via Dashboard
1. Navigate to `http://localhost:5000`
2. Click "Import from File" to upload the JSON file.
3. Monitor real-time status updates
4. View scraped articles in the table




