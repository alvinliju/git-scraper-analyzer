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
                """
                ALTER TABLE repo_queue ADD COLUMN IF NOT EXISTS processed BOOLEAN DEFAULT FALSE;

                -- Create repos table ONLY if it doesn't exist
                CREATE TABLE IF NOT EXISTS repos (
                    id SERIAL PRIMARY KEY,
                    repo_id BIGINT UNIQUE NOT NULL,
                    repo_name TEXT,
                    stars INTEGER DEFAULT 0,
                    forks INTEGER DEFAULT 0,
                    open_issues INTEGER DEFAULT 0,
                    closed_issues INTEGER DEFAULT 0,
                    subscribers INTEGER DEFAULT 0,
                    commits_last_30_days INTEGER DEFAULT 0,
                    contributors_count INTEGER DEFAULT 0,
                    activity_score INTEGER,
                    enriched_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
                
                -- Add any missing columns (for schema migrations)
                DO $$ 
                BEGIN
                    -- Add columns that might be missing (idempotent)
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                   WHERE table_name='repos' AND column_name='subscribers') THEN
                        ALTER TABLE repos ADD COLUMN subscribers INTEGER DEFAULT 0;
                    END IF;
                    
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                   WHERE table_name='repos' AND column_name='commits_last_30_days') THEN
                        ALTER TABLE repos ADD COLUMN commits_last_30_days INTEGER DEFAULT 0;
                    END IF;
                    
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                   WHERE table_name='repos' AND column_name='contributors_count') THEN
                        ALTER TABLE repos ADD COLUMN contributors_count INTEGER DEFAULT 0;
                    END IF;
                    
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                   WHERE table_name='repos' AND column_name='activity_score') THEN
                        ALTER TABLE repos ADD COLUMN activity_score INTEGER;
                    END IF;
                    
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                   WHERE table_name='repos' AND column_name='enriched_at') THEN
                        ALTER TABLE repos ADD COLUMN enriched_at TIMESTAMP DEFAULT NOW();
                    END IF;
                    
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                   WHERE table_name='repos' AND column_name='updated_at') THEN
                        ALTER TABLE repos ADD COLUMN updated_at TIMESTAMP DEFAULT NOW();
                    END IF;
                END $$;
                """
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

        # Create related tables (languages, topics, dependencies) - repos table must exist first
        async with self.pool.acquire() as conn:
            # Drop enriched_repos table if it exists (we're using repos now)
            await conn.execute("DROP TABLE IF EXISTS enriched_repos CASCADE;")

            # Languages table (many-to-many with repos)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS repo_languages (
                    id SERIAL PRIMARY KEY,
                    repo_id BIGINT NOT NULL,
                    language_name TEXT NOT NULL,
                    size_bytes BIGINT,
                    percentage NUMERIC(5, 2),
                    FOREIGN KEY (repo_id) REFERENCES repos(repo_id) ON DELETE CASCADE,
                    UNIQUE(repo_id, language_name)
                )
            """)

            # Topics table (many-to-many with repos)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS repo_topics (
                    id SERIAL PRIMARY KEY,
                    repo_id BIGINT NOT NULL,
                    topic_name TEXT NOT NULL,
                    FOREIGN KEY (repo_id) REFERENCES repos(repo_id) ON DELETE CASCADE,
                    UNIQUE(repo_id, topic_name)
                )
            """)

            # Dependencies table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS repo_dependencies (
                    id SERIAL PRIMARY KEY,
                    repo_id BIGINT NOT NULL,
                    package_name TEXT NOT NULL,
                    requirements TEXT,
                    manifest_filename TEXT,
                    FOREIGN KEY (repo_id) REFERENCES repos(repo_id) ON DELETE CASCADE
                )
            """)

            # Create indexes for better query performance
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_repos_repo_id ON repos(repo_id)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_repo_languages_repo_id ON repo_languages(repo_id)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_repo_topics_repo_id ON repo_topics(repo_id)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_repo_dependencies_repo_id ON repo_dependencies(repo_id)
            """)

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

    async def bulk_save_enriched_repos(self, enriched_data_list):
        """
        Bulk save enriched repository data including languages, topics, and dependencies.
        
        Args:
            enriched_data_list: List of dicts, each containing:
                - repo_id: The repository ID
                - repo_name: The repository name (owner/repo)
                - parsed_data: The parsed data from parse_repo_data()
        """
        if not enriched_data_list:
            return

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Prepare data for bulk insert
                repos_to_insert = []
                languages_to_insert = []
                topics_to_insert = []
                dependencies_to_insert = []

                for item in enriched_data_list:
                    repo_id = item['repo_id']
                    repo_name = item['repo_name']
                    data = item['parsed_data']
                    activity_score = item.get('activity_score', 0)

                    # Main repo data
                    repos_to_insert.append((
                        repo_id,
                        repo_name,
                        data.get('stars', 0),
                        data.get('forks', 0),
                        data.get('open_issues', 0),
                        data.get('closed_issues', 0),
                        data.get('subscribers', 0),
                        data.get('commits_last_30_days', 0),
                        data.get('contributors_count', 0),
                        activity_score
                    ))

                    # Languages
                    languages = data.get('languages', {})
                    language_percentages = data.get('language_percentages', {})
                    for lang_name, size in languages.items():
                        percentage = language_percentages.get(lang_name, 0)
                        languages_to_insert.append((
                            repo_id,
                            lang_name,
                            size,
                            float(percentage)
                        ))

                    # Topics
                    topics = data.get('topics', {})
                    for topic_name in topics.keys():
                        topics_to_insert.append((
                            repo_id,
                            topic_name
                        ))

                    # Dependencies
                    dependencies = data.get('dependencies', [])
                    for dep in dependencies:
                        dependencies_to_insert.append((
                            repo_id,
                            dep.get('package', ''),
                            dep.get('requirements', ''),
                            dep.get('manifest', '')
                        ))

                # Bulk insert enriched repos
                if repos_to_insert:
                    await conn.executemany("""
                        INSERT INTO repos (
                            repo_id, repo_name, stars, forks, open_issues, 
                            closed_issues, subscribers, commits_last_30_days, 
                            contributors_count, activity_score
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                        ON CONFLICT (repo_id) DO UPDATE SET
                            stars = EXCLUDED.stars,
                            forks = EXCLUDED.forks,
                            open_issues = EXCLUDED.open_issues,
                            closed_issues = EXCLUDED.closed_issues,
                            subscribers = EXCLUDED.subscribers,
                            commits_last_30_days = EXCLUDED.commits_last_30_days,
                            contributors_count = EXCLUDED.contributors_count,
                            activity_score = EXCLUDED.activity_score,
                            updated_at = NOW()
                    """, repos_to_insert)

                # Bulk insert languages (delete old ones first, then insert new)
                if languages_to_insert:
                    repo_ids = [item['repo_id'] for item in enriched_data_list]
                    await conn.execute("""
                        DELETE FROM repo_languages 
                        WHERE repo_id = ANY($1::bigint[])
                    """, repo_ids)
                    
                    await conn.executemany("""
                        INSERT INTO repo_languages (repo_id, language_name, size_bytes, percentage)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (repo_id, language_name) DO UPDATE SET
                            size_bytes = EXCLUDED.size_bytes,
                            percentage = EXCLUDED.percentage
                    """, languages_to_insert)

                # Bulk insert topics (delete old ones first, then insert new)
                if topics_to_insert:
                    repo_ids = [item['repo_id'] for item in enriched_data_list]
                    await conn.execute("""
                        DELETE FROM repo_topics 
                        WHERE repo_id = ANY($1::bigint[])
                    """, repo_ids)
                    
                    await conn.executemany("""
                        INSERT INTO repo_topics (repo_id, topic_name)
                        VALUES ($1, $2)
                        ON CONFLICT (repo_id, topic_name) DO NOTHING
                    """, topics_to_insert)

                # Bulk insert dependencies (delete old ones first, then insert new)
                if dependencies_to_insert:
                    repo_ids = [item['repo_id'] for item in enriched_data_list]
                    await conn.execute("""
                        DELETE FROM repo_dependencies 
                        WHERE repo_id = ANY($1::bigint[])
                    """, repo_ids)
                    
                    await conn.executemany("""
                        INSERT INTO repo_dependencies (repo_id, package_name, requirements, manifest_filename)
                        VALUES ($1, $2, $3, $4)
                    """, dependencies_to_insert)

                print(f"  💾 Bulk saved {len(repos_to_insert)} enriched repos with "
                      f"{len(languages_to_insert)} languages, {len(topics_to_insert)} topics, "
                      f"and {len(dependencies_to_insert)} dependencies")

    async def mark_repos_as_processed(self, repo_ids):
        """Mark repos in repo_queue as processed."""
        if not repo_ids:
            return
        
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE repo_queue 
                SET processed = TRUE 
                WHERE repo_id = ANY($1::bigint[])
            """, repo_ids)
            print(f"  ✅ Marked {len(repo_ids)} repos as processed")

    async def execute_query(self, sql_query: str, limit: int = 100):
        """
        Execute a SELECT query and return results.
        Safe wrapper that only allows SELECT queries.
        """
        sql_upper = sql_query.upper().strip()
        
        # Ensure it's a SELECT query
        if not sql_upper.startswith('SELECT'):
            raise ValueError("Only SELECT queries are allowed")
        
        # Add LIMIT if not present
        if 'LIMIT' not in sql_upper:
            sql_query = sql_query.rstrip(';') + f' LIMIT {limit}'
        
        async with self.pool.acquire() as conn:
            # Set timeout
            await conn.execute("SET statement_timeout = '5s'")
            
            # Execute query
            rows = await conn.fetch(sql_query)
            
            # Get column names from first row if available
            if rows:
                columns = list(rows[0].keys())
                # Convert rows to lists for JSON serialization
                results = [list(row.values()) for row in rows]
            else:
                # If no rows, get columns from query metadata
                # For asyncpg, we need to execute and check
                columns = []
                results = []
            
            return columns, results
    
    async def get_all_repos(self, limit: int = 100):
        """Get all repos with limit"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM repos 
                ORDER BY stars DESC 
                LIMIT $1
            """, limit)
            return [dict(row) for row in rows]
    
    async def get_repo_by_id(self, repo_id: int):
        """Get a single repo by repo_id"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM repos WHERE repo_id = $1
            """, repo_id)
            return dict(row) if row else None
    
    async def get_stats(self):
        """Get database statistics"""
        async with self.pool.acquire() as conn:
            total_repos = await conn.fetchval("SELECT COUNT(*) FROM repos")
            
            # Calculate healthy repos (active with good activity)
            healthy_repos = await conn.fetchval("""
                SELECT COUNT(*) FROM repos 
                WHERE commits_last_30_days > 0 AND activity_score > 10
            """)
            
            avg_stars = await conn.fetchval("SELECT AVG(stars) FROM repos WHERE stars > 0")
            
            # Most popular language
            lang_row = await conn.fetchrow("""
                SELECT language_name, COUNT(*) as count
                FROM repo_languages
                GROUP BY language_name
                ORDER BY count DESC
                LIMIT 1
            """)
            
            most_popular_language = lang_row['language_name'] if lang_row else None
            
            return {
                "total_repos": total_repos or 0,
                "healthy_repos": healthy_repos or 0,
                "avg_stars": float(avg_stars) if avg_stars else 0.0,
                "most_popular_language": most_popular_language
            }


