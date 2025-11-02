# GitHub Scraper

Scrapes GitHub repos, analyzes health metrics, stores in Postgres, serves via REST API.

## What it does

- Scrapes 900 repos with >1000 stars
- Fetches detailed stats (commits, issues, contributors)
- Calculates health metrics (activity, freshness)
- Stores in Postgres
- Exposes queryable API
- Handles rate limits properly
- Resumes on failure

## Stack

Python, asyncio, aiohttp, Postgres, Flask

## Run it

```bash
# Setup
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=pwd --name pgdb postgres
python -m venv myenv && source myenv/bin/activate
pip install -r requirements.txt

# Add token
echo "GITHUB_TOKEN=your_token_here" > .env

# Scrape
python main.py                          # Get 900 repos (~5 mins)
python parse_store_in_postgres.py      # Load to DB

# Serve
python api.py                           # API on :3000
curl localhost:3000/stats
```

## Automation

```bash
chmod +x cron_job.py
crontab -e
# Add: 0 */6 * * * cd /path && ./myenv/bin/python cron_job.py
```

## Performance

- 900 repos in ~5 minutes
- Rate limited to 75 req/min (under GitHub's 83/min limit)
- Uses async/await for concurrency
- Checkpoint-based resume on failure

## API Endpoints

GET /repos          - All repos
GET /repos/{id}     - Single repo
GET /stats          - Summary stats
GET /repos/top      - Top by stars CONFLICT for idempotency)
- Building resumable data pipelines
- Production error handling

## Why it matters

idk it seemed intresting enough to spend 2 nights spoiling my sleep and messing my academics....

## Files

- `main.py` - Scraper with async rate limiting
- `parse_store_in_postgres.py` - DB loader
- `api.py` - REST API
- `cron_job.py` - Automated pipeline
- `clean_db.py` - Remove duplicates

## License

MIT
