import os
import hashlib
from dotenv import load_dotenv
from google import genai
from helpers.lru import LRUCache
import asyncio

load_dotenv()

CACHE_CAPACITY = 100
query_cache = LRUCache(CACHE_CAPACITY)

gemini_api_key = os.getenv('GEMINI_API_KEY')
if not gemini_api_key:
    raise ValueError("GEMINI_API_KEY not found in environment variables")

client = genai.Client(api_key=gemini_api_key)

SCHEMA_CONTEXT = """
You are a SQL query generator for a GitHub repository database.

Main Table: repos
Columns:
- id (SERIAL PRIMARY KEY)
- repo_id (BIGINT UNIQUE NOT NULL) - GitHub repository ID
- repo_name (TEXT) - Format: "owner/repo"
- stars (INTEGER) - Stargazers count
- forks (INTEGER) - Fork count
- open_issues (INTEGER) - Open issues count
- closed_issues (INTEGER) - Closed issues count
- subscribers (INTEGER) - Watchers/subscribers count
- commits_last_30_days (INTEGER) - Commits in last 30 days
- contributors_count (INTEGER) - Number of contributors
- activity_score (INTEGER) - Activity score from GitHub Archive
- enriched_at (TIMESTAMP) - When data was enriched
- updated_at (TIMESTAMP) - Last update time

Related Tables:
- repo_languages (repo_id, language_name, size_bytes, percentage)
- repo_topics (repo_id, topic_name)
- repo_dependencies (repo_id, package_name, requirements, manifest_filename)

Examples:
Q: "Show me top 10 Python repos"
A: SELECT repo_name, stars, forks FROM repos WHERE repo_id IN (SELECT repo_id FROM repo_languages WHERE language_name='Python') ORDER BY stars DESC LIMIT 10;

Q: "How many repos have more than 1000 stars?"
A: SELECT COUNT(*) FROM repos WHERE stars > 1000;

Q: "Find repos with high activity in the last 30 days"
A: SELECT repo_name, stars, commits_last_30_days, activity_score FROM repos WHERE commits_last_30_days > 50 ORDER BY commits_last_30_days DESC LIMIT 20;

Q: "Show me repos about machine learning"
A: SELECT DISTINCT r.repo_name, r.stars FROM repos r JOIN repo_topics rt ON r.repo_id = rt.repo_id WHERE rt.topic_name ILIKE '%machine learning%' OR rt.topic_name ILIKE '%ml%' ORDER BY r.stars DESC LIMIT 10;

Q: "What are the most popular languages?"
A: SELECT language_name, COUNT(*) as repo_count FROM repo_languages GROUP BY language_name ORDER BY repo_count DESC LIMIT 10;

Generate ONLY the SQL query, no explanations, no markdown formatting.
"""


def generate_sql_query(prompt: str) -> str:
    """Generate SQL query from natural language using Gemini"""
    try:
        response = client.models.generate_content(
            model="gemini-2.0-flash-exp",
            contents=SCHEMA_CONTEXT + "\n\nQ: " + prompt + "\nA:"
        )
        sql = response.text.strip()
        
        # Clean up markdown code blocks if present
        sql = sql.replace('ql', '').replace('```', '').strip()
        
        return sql
    except Exception as e:
        raise ValueError(f"Failed to generate SQL query: {e}")


DANGEROUS_KEYWORDS = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'TRUNCATE', 'CREATE', 'GRANT', 'REVOKE']


def is_safe_to_execute(query: str) -> bool:
    """Check if SQL query is safe to execute (SELECT only)"""
    sql_query = query.upper().strip()
    
    # Must start with SELECT
    if not sql_query.startswith('SELECT'):
        return False
    
    # Check for dangerous keywords
    for word in DANGEROUS_KEYWORDS:
        if word in sql_query:
            return False
    
    return True


def get_cache_key(question: str) -> str:
    """Generate a consistent cache key from question"""
    normalized = question.lower().strip()
    return hashlib.md5(normalized.encode()).hexdigest()


async def execute_sql_query(question: str, db_helper, limit: int = 100):
    """
    Execute SQL query using db_helper.
    Returns: (columns, results)
    """
    sql_query = generate_sql_query(question)
    print(f"Generated SQL: {sql_query}")
    
    if not is_safe_to_execute(sql_query):
        raise ValueError(f"Unsafe SQL generated: {sql_query}")
    
    # Execute using db_helper
    columns, results = await db_helper.execute_query(sql_query, limit=limit)
    
    return columns, results


async def execute_sql_query_cached(question: str, db_helper, limit: int = 100):
    """Execute query with LRU caching"""
    cache_key = get_cache_key(question)
    
    # Check cache
    cached_result = query_cache.get(cache_key)
    if cached_result != -1:  # Cache hit
        print(f"✓ Cache hit for: {question}")
        return cached_result
    
    # Cache miss - execute query
    print(f"✗ Cache miss for: {question}")
    columns, results = await execute_sql_query(question, db_helper, limit)
    
    # Store in cache
    query_cache.put(cache_key, (columns, results))
    
    return columns, results


# Synchronous wrapper for Flask (since Flask is sync)
def execute_sql_query_cached_sync(question: str, db_helper, limit: int = 100):
    """Synchronous wrapper for Flask"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(execute_sql_query_cached(question, db_helper, limit))
    finally:
        loop.close()## 4. Create Flask API (`api.py`):

