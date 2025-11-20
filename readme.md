# GitScraper

High-performance async scraper for GitHub Archive data. Downloads hourly event archives, extracts repo activity, and stores it in PostgreSQL with resumable state tracking.

## What it does

- 📥 Downloads GitHub Archive files from [gharchive.org](https://www.gharchive.org)
- 🔄 Processes events to extract repo activity counts
- 💾 Stores data in PostgreSQL with deadlock-safe batching
- ✅ Tracks scraping state in database (resumable on restart)
- ⚡ Concurrent downloads with configurable limits

## Quick Start

# Start PostgreSQL
cd db && docker-compose up -d

# Set environment variables
export DB_HOST=localhost
export DB_PORT=5432
export DB_USER=postgres
export DB_PASSWORD=pwd
export DB_NAME=postgres

# Run scraper
python discovery.py

## Architecture

discovery.py          → Main scraper (downloads & processes)
helpers/db_helper.py  → Database operations & queue management
processor.py          → Future: aggregate raw data into ins

- **Resumable**: Database tracks progress, restarts continue from last position
- **Concurrent**: Async downloads with semaphore-based rate limiting
- **Robust**: Retry logic, deadlock prevention, timeout handling
- **Scalable**: Batch processing, connection pooling

## Tech Stack

- Python 3.13
- asyncpg (PostgreSQL async driver)
- aiohttp (async HTTP client)
- PostgreSQL 15

---

*Built for analyzing GitHub activity at scale.*


