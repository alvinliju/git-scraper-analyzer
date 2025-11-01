import psycopg2
import json 

conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="pwd",
    port=5432,
)

cur = conn.cursor()

def parse_and_store_in_postgres(repo_details: list):
    for repo in repo_details:
        name = repo['name']
        link = repo['link']
        likes = repo['likes']
        forks = repo['forks']
        watchers = repo['watchers']
        open_issues_count = repo['open_issues_count']
        language = repo['language']
        created_at = repo['created_at']
        maintainers = repo['maintainers']
        last_commit_date = repo['last-commit-date']
        total_commits_last_year = repo['total_commits_last_year']
        recent_commits_fetched = repo['recent-commits-fetched']