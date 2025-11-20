import asyncio
import helpers.db_helper as db_helper
from dotenv import load_dotenv
load_dotenv()
import requests
import os
import aiohttp as aiohttp
from datetime import datetime, timedelta 

GRAPHQL_URL = "https://api.github.com/graphql"

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

QUERY = """
query ($owner: String!, $name: String!) {
  repository(owner: $owner, name: $name) {
    stargazerCount
    forkCount
    openIssues: issues(states: OPEN) { totalCount }
    closedIssues: issues(states: CLOSED) { totalCount }
    watchers { totalCount }
    defaultBranchRef {
      target {
        ... on Commit {
          history(first: 30, since: "%s") {
            totalCount
          }
        }
      }
    }
    mentionableUsers(first: 100) {
      totalCount
    }
    languages(first: 10) {
      edges {
        node { name }
        size
      }
    }
    repositoryTopics(first: 20) {
      nodes {
        topic { name }
      }
    }
  }
}
"""

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
            results = await conn.fetchall()
            return results

    async def enrich_repo(self, repo_id, owner, name):
        async with aiohttp.ClientSession() as session:
            thirty_days_ago = (datetime.now() - timedelta(days=30)).isoformat()

            query = QUERY % thirty_days_ago
            headers = {
                "Authorization": f"Bearer {GITHUB_TOKEN}",
                "Content-Type": "application/json",
            }
            response = await session.post(
            GRAPHQL_URL,
            json={"query": query, "variables": {"owner": owner, "name": name}},
            headers=headers
            )

            if response.status != 200:
                print(f"Error enriching repo {repo_id}: {response.status}")
                return None

            data = await response.json()
            return data['data']['repository']

    async def parse_repo_data(self, repo_data):
        commits = []
        if repo_data.get('defaultBranchRef') and repo_data['defaultBranchRef'].get('target'):
            history = repo_data['defaultBranchRef']['target'].get('history', {})
            commits = [node['committedDate'] for node in history.get('nodes', [])]

        languages = {}
        for edge in repo_data['languages']['edges']:
            lang_name = edge['node']['name']
            size = edge['size']
            languages[lang_name] = size

        topics = {}
        for node in repo_data['repositoryTopics']['nodes']:
            topic_name = node['topic']['name']
            topics[topic_name] = topics.get(topic_name, 0) + 1
        
        stargazer_count = repo_data['stargazerCount']
        fork_count = repo_data['forkCount']
        open_issues = repo_data['openIssues']['totalCount']
        closed_issues = repo_data['closedIssues']['totalCount']
        watchers = repo_data['watchers']['totalCount']
        default_branch_ref = repo_data['defaultBranchRef']['target']['history']['totalCount']
        mentionable_users = repo_data['mentionableUsers']['totalCount']
        languages = repo_data['languages']['edges']

        return {
            'stargazer_count': stargazer_count,
            'fork_count': fork_count,
            'open_issues': open_issues,
            'closed_issues': closed_issues,
            'watchers': watchers,
            'default_branch_ref': default_branch_ref,
            'mentionable_users': mentionable_users,
            'languages': languages
        }
            