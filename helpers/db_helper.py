import asyncpg
import os

from dotenv import load_dotenv
load_dotenv()
import asyncio
class DBHelper:
    def __init__(self):
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )

        async with self.pool.acquire() as conn:
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS repo_activity (
                    id SERIAL PRIMARY KEY,
                    repo_id BIGINT UNIQUE,
                    repo_name TEXT,
                    activity_count INTEGER
                )"""
            )

            await conn.execute(
                """CREATE TABLE IF NOT EXISTS repo_queue (
                    id SERIAL PRIMARY KEY,
                    repo_id BIGINT UNIQUE,
                    repo_name TEXT,
                    activity_count INTEGER
                )"""
            )

            await conn.execute(
                
            )

    async def save_repo_id_to_queue(self, repo_activity):
        MAX_RETRIES = 3
        retries = 0
        while retries < MAX_RETRIES:
            try:
                async with self.pool.acquire() as conn:
                    if not repo_activity:
                        return
                    BATCH_SIZE = 1000
                    
                    #sort by activity count
                    sorted_repo_actitivity = sorted(repo_activity.items())
                    for i in range(0, len(sorted_repo_actitivity), BATCH_SIZE):
                        batch = sorted_repo_actitivity[i:i+BATCH_SIZE]
                        values = [
                            (repo_id, data['name'], data['count'])
                            for repo_id, data in batch
                        ]
                        await conn.executemany("""
                            INSERT INTO repo_queue (repo_id, repo_name, activity_count)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (repo_id) DO UPDATE 
                            SET activity_count = repo_queue.activity_count + EXCLUDED.activity_count
                        """, values)

                    print(f"  💾 Saved {len(batch)} repos to DB")
                    return

            except 'OperationalError' as e:
                if "deadlock detected" in str(e).lower():
                    retries += 1
                    await asyncio.sleep(2 * retries) ##backoff
                    continue
                else:
                    raise
            except Exception as e:
                raise e

