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
                """
                CREATE TABLE IF NOT EXISTS url_queue(
                    id SERIAL PRIMARY KEY,
                    url TEXT UNIQUE NOT NULL,
                    done BOOLEAN DEFAULT FALSE,
                    scraped_at TIMESTAMP,   
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_url_queue_done ON url_queue(done)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_url_queue_url ON url_queue(url)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_url_queue_scraped_at ON url_queue(scraped_at)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_url_queue_created_at ON url_queue(created_at)"
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

    async def bulk_insert_urls(self, urls):
        async with self.pool.acquire() as conn:
            await conn.executemany("""
            INSERT INTO url_queue (url, done)
            VALUES ($1, FALSE)
            ON CONFLICT (url) DO NOTHING
            """, [(url,) for url in urls])
            print(f"  📝 Inserted {len(urls)} URLs into queue")

    async def mark_url_done(self, url):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE url_queue
                SET done = TRUE,
                    scraped_at = NOW()
                WHERE url = $1
            """, url)
    
    async def get_pending_urls(self, limit=None):
        async with self.pool.acquire() as conn:
            if limit:
                rows = await conn.fetch("""
                 SELECT url FROM url_queue WHERE done = FALSE ORDER BY created_at LIMIT $1
                """, limit)
            else:
                rows = await conn.fetch("""
                SELECT url FROM url_queue WHERE done = FALSE ORDER BY created_at
                """)
            return [row['url'] for row in rows]


