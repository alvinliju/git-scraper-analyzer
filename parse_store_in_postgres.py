import psycopg2
import json 
from datetime import datetime

conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="pwd",
    port=5432,
)

cur = conn.cursor()

def parse_and_store_in_postgres(repo_details: list):
    repo_details = [repo for repo in repo_details if repo is not None]
    print(f"Valid repos: {len(repo_details)}")
    for repo in repo_details:
        name = repo['name']
        link = repo['link']
        stars = repo['likes']
        forks = repo['forks']
        watchers = repo['watchers']
        open_issues = repo['open_issues_count']
        language = repo['language']
        created_at = repo['created_at']
        owner = repo['maintainers']
        last_commit_date = repo['last-commit-date']
        commits_last_year = repo['total_commits_last_year']

        ##derived fields
        # Line 31-33 should be:
        repo_age_days = (datetime.now() - datetime.strptime(created_at, '%Y-%m-%dT%H:%M:%SZ')).days
        days_since_last_commit = (datetime.now() - datetime.strptime(last_commit_date, '%Y-%m-%dT%H:%M:%SZ')).days
        is_active = days_since_last_commit < 30
        is_healthy =commits_last_year >= 100 and is_active  

        cur.execute(
            """ 
            INSERT INTO repos (name, link, owner, stars, forks, watchers, open_issues, language, created_at, last_commit_date, commits_last_year, repo_age_days, days_since_last_commit, is_active, is_healthy)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (name) DO UPDATE SET
                stars = EXCLUDED.stars,
                forks = EXCLUDED.forks,
                watchers = EXCLUDED.watchers,
                open_issues = EXCLUDED.open_issues,
                last_commit_date = EXCLUDED.last_commit_date,
                commits_last_year = EXCLUDED.commits_last_year,
                days_since_last_commit = EXCLUDED.days_since_last_commit,
                is_active = EXCLUDED.is_active,
                is_healthy = EXCLUDED.is_healthy
            """, 
            (name, link, owner, stars, forks, watchers, open_issues, language, created_at, last_commit_date, commits_last_year, repo_age_days, days_since_last_commit, is_active, is_healthy)
        )
        conn.commit()
    print(f"All {len(repo_details)} repos parsed and stored in postgres")

if __name__ == "__main__":
    with open('repo_details.json', 'r', encoding='utf-8') as f:
        repo_details = json.load(f)
    print(f"Total repos loaded: {len(repo_details)}")
    print("Starting to parse and store in postgres...")
    parse_and_store_in_postgres(repo_details)
    print("All repos parsed and stored in postgres successfully")