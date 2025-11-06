import asyncio
import aiohttp
import psycopg2
from aiolimiter from AsyncLimiter
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
BASE_URL="https://api.github.com"

if not GITHUB_TOKEN:
    raise ValueError("GITHUB_TOKEN not found in environment variables")

class GraphDiscoveryEngine:
    def __init__(self):
        self.conn = db_connection()
        self.rate_limiter = AsyncLimiter(max_rate=75, time_period=60)
        self.discovered = set()
        self.to_explore = []

    async def discover_from_startgazers(self, repo_name, session):
        
