import asyncio
import helpers.db_helper as db_helper
from dotenv import load_dotenv
load_dotenv()

class Processor:
    def __init__(self):
        self.db_helper = db_helper.DBHelper()

    async def setup(self):
        await self.db_helper.connect()
        print("Database connected in processor python")

    async def process_repo_queue(self):
        async with self.db_helper.pool.acquire() as conn:
            await conn.execute("""
            SELECT * FROM repo_queue
            ORDER BY activity_count DESC
            LIMIT 1000
            """)
            rows = await conn.fetchall()
            for row in rows:
                print(f"Processing repo: {row['repo_id']}")
                await self.process_repo_activity(row['repo_id'])

    async def process_repo_activity(self, repo_id):
        async with self.db_helper.pool.acquire() as conn:
            await conn.execute("""
            SELECT * FROM repo_activity
            WHERE repo_id = $1
            """, repo_id)
            row = await conn.fetchone()