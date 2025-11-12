import psycopg2
import json
from dotenv import load_dotenv
import logging

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="pwd",
        port=5432,
    )
    return conn


def get_scraped_repos_name():
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM repos")
        return {row[0] for row in cur.fetchall()}
    except Exception as e:
        print(f"Error getting scraped repos names: {e}")
        return []
    finally:
        cur.close()
        conn.close()

def save_raw_repos_to_db(repos:list):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        logger.debug(f"[SAVE RAW] Starting to save {len(repos)} repos to database")
        
        # Debug: Show sample of what we're saving
        if repos:
            sample_repo = repos[0]
            logger.debug(f"[SAVE RAW] Sample repo keys: {list(sample_repo.keys())[:10]}")
            logger.debug(f"[SAVE RAW] Sample repo - Name: {sample_repo.get('name', 'N/A')}, HTML URL: {sample_repo.get('html_url', 'N/A')}")
        
        inserted_count = 0
        skipped_count = 0
        for repo in repos:
            try:
                # Transform GitHub API response to match parser's expected format
                owner_obj = repo.get('owner', {})
                owner_name = owner_obj.get('login', '') if isinstance(owner_obj, dict) else str(owner_obj)
                
                cur.execute(
                    """INSERT INTO raw_repos 
                       (name, link, likes, forks, watchers, open_issues_count, language, 
                        created_at, maintainers, last_commit_date, total_commits_last_year) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (name) DO NOTHING""",
                    (
                        repo.get('name', ''),
                        repo.get('html_url', ''),
                        repo.get('stargazers_count', 0),
                        repo.get('forks_count', 0),
                        repo.get('watchers_count', 0),
                        repo.get('open_issues_count', 0),
                        repo.get('language', 'Unknown'),
                        repo.get('created_at', ''),
                        owner_name,
                        repo.get('pushed_at', ''),
                        0  # total_commits_last_year - will be calculated by parser
                    )
                )
                inserted_count += 1
            except Exception as e:
                logger.warning(f"[SAVE RAW] Skipped repo {repo.get('name', 'unknown')}: {e}")
                skipped_count += 1
                continue
        
        conn.commit()
        logger.debug(f"[SAVE RAW] Successfully committed {inserted_count} repos, skipped {skipped_count}")
        
        # Verify by counting records
        cur.execute("SELECT COUNT(*) FROM raw_repos")
        total_count = cur.fetchone()[0]
        logger.debug(f"[SAVE RAW] Total records in raw_repos table after save: {total_count}")
        
        print(f"Raw repos saved to db: {inserted_count} (skipped: {skipped_count})")
    except Exception as e:
        logger.error(f"[SAVE RAW] Error saving raw repos to db: {e}", exc_info=True)
        print(f"Error saving raw repos to db: {e}")
        return False
    finally:
        cur.close()
        conn.close()
    return True

def get_raw_repos_from_db():

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        # First, check how many records exist
        cur.execute("SELECT COUNT(*) FROM raw_repos")
        total_count = cur.fetchone()[0]
        logger.debug(f"[FETCH RAW] Total records in raw_repos table: {total_count}")
        
        # Fetch all columns as dict-like structure for parser
        cur.execute("""
            SELECT name, link, likes, forks, watchers, open_issues_count, language, 
                   created_at, maintainers, last_commit_date, total_commits_last_year
            FROM raw_repos
        """)
        
        results = cur.fetchall()
        # Convert to list of dicts matching parser's expected format
        fetched_data = [
            {
                'name': row[0],
                'link': row[1],
                'likes': row[2],
                'forks': row[3],
                'watchers': row[4],
                'open_issues_count': row[5],
                'language': row[6],
                'created_at': row[7],
                'maintainers': row[8],
                'last-commit-date': row[9],  # Note: hyphen for parser
                'total_commits_last_year': row[10]
            }
            for row in results
        ]
        
        logger.debug(f"[FETCH RAW] Successfully fetched {len(fetched_data)} records from database")
        
        # Debug: Show sample of what we fetched
        if fetched_data:
            sample = fetched_data[0]
            logger.debug(f"[FETCH RAW] Sample fetched data - Name: {sample.get('name')}, Link: {sample.get('link')}")
        
        return fetched_data
    except Exception as e:
        logger.error(f"[FETCH RAW] Error getting raw repos from db: {e}", exc_info=True)
        print(f"Error getting raw repos from db: {e}")
        return []
    finally:
        cur.close()
        conn.close()

def save_to_queue(repos:set):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        inserted_count = 0
        for repo in repos:
            try:
                repo_id = repo.get('id')
                repo_full_name = repo.get('full_name', '')

                if not repo_id:
                    logger.warning(f"[SAVE TO QUEUE] Skipping repo with no ID: {repo.get('name', 'unknown')}")
                    continue
                cur.execute(
                    """INSERT INTO repo_queue 
                       (repo_id, repo_full_name, status, discovered_at) 
                       VALUES (%s, %s, 'pending', CURRENT_TIMESTAMP)
                       ON CONFLICT (repo_id) DO NOTHING""",
                    (repo_id, repo_full_name)
                )
                inserted_count += 1
            except Exception as e:
                logger.warning(f"[SAVE TO QUEUE] Skipped repo {repo.get('name', 'unknown')}: {e}")
                continue
        conn.commit()
        logger.debug(f"[SAVE TO QUEUE] Successfully committed {inserted_count} repos, skipped {skipped_count}")
        print(f"Repos saved to queue: {inserted_count} (skipped: {skipped_count})")
    except Exception as e:
        logger.error(f"[SAVE TO QUEUE] Error saving to queue: {e}", exc_info=True)
        print(f"Error saving to queue: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

def get_pending_repos_from_queue(limt:int=100):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT repo_id, repo_full_name FROM repo_queue WHERE status = 'pending' LIMIT %s", (limit,))
        return cur.fetchall()
    except Exception as e:
        logger.error(f"[GET PENDING REPOS FROM QUEUE] Error getting pending repos from queue: {e}", exc_info=True)
        print(f"Error getting pending repos from queue: {e}")
        return []
    finally:
        cur.close()
        conn.close()

def mark_repo_in_progress(repo_id:int):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
        UPDATE repo_queue
        SET status = 'in_progress', updated_at = CURRENT_TIMESTAMP
        WHERE repo_id = %s
        """, (repo_id,))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"[MARK REPO IN PROGRESS] Error marking repo in progress: {e}", exc_info=True)
        print(f"Error marking repo in progress: {e}")
        return False
    finally:
        cur.close()
        conn.close()

def mark_repo_completed(repo_id:int):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
        UPDATE repo_queue
        SET status = 'completed', updated_at = CURRENT_TIMESTAMP
        WHERE repo_id = %s
        """, (repo_id,))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"[MARK REPO IN PROGRESS] Error marking repo in progress: {e}", exc_info=True)
        print(f"Error marking repo in progress: {e}")
        return False
    finally:
        cur.close()
        conn.close()

def mark_repo_failed(repo_id: int, error_message: str = None):
    """Mark a repo as failed and increment retry count."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE repo_queue 
            SET status = 'failed', 
                retry_count = retry_count + 1,
                last_error = %s,
                last_attempt_at = CURRENT_TIMESTAMP
            WHERE repo_id = %s
        """, (error_message, repo_id))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"[MARK FAILED] Error marking repo {repo_id} as failed: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()  