import asyncio
import helpers.db_helper as db_helper
from dotenv import load_dotenv
load_dotenv()
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
   
    def __init__(self, MAX_CON=5):
        self.db_helper = db_helper.DBHelper()
        self.semaphore = asyncio.Semaphore(MAX_CON)

    async def setup(self):
        await self.db_helper.connect()
        print("Database connected in processor python")

    async def process_repo_queue(self, batch_size):
        async with self.db_helper.pool.acquire() as conn:
            results = await conn.fetch("""
            SELECT * FROM repo_queue
            WHERE processed = FALSE
            ORDER BY activity_count DESC
            LIMIT $1
            """, batch_size)
        
            return results

    async def enrich_repo(self, repo_id, owner, name):
        async with self.semaphore:
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
                    print(f"❌ Repo {repo_id}: HTTP {response.status}")
                    return None

                data = await response.json()
                
                # Check for GraphQL errors
                if 'errors' in data:
                    # Don't print - already handled below
                    pass
                
                if 'data' not in data or data['data']['repository'] is None:
                    return None
                
                return data['data']['repository']

    def parse_repo_data(self, repo_data):
        commits = []
        if repo_data.get('defaultBranchRef') and repo_data['defaultBranchRef'].get('target'):
            history = repo_data['defaultBranchRef']['target'].get('history', {})
            commits = [node['committedDate'] for node in history.get('nodes', [])]

        languages = {}
        for edge in repo_data['languages']['edges']:
            lang_name = edge['node']['name']
            size = edge['size']
            languages[lang_name] = size

        total_size = sum(languages.values())
        language_percentages = {lang_name: (size/ total_size) * 100 for lang_name, size in languages.items()}

        topics = {}
        for node in repo_data['repositoryTopics']['nodes']:
            topic_name = node['topic']['name']
            topics[topic_name] = topics.get(topic_name, 0) + 1

        dependencies = []
        for manifest in repo_data.get('dependencyGraphManifests', {}).get('nodes', []) or []:
            for dep in manifest.get('dependencies', {}).get('nodes', []) or []:
                dependencies.append({
                    'package': dep['packageName'],
                    'requirements': dep.get('requirements', ''),
                    'manifest': manifest['filename']
                })
        

        return {
            'stars': repo_data['stargazerCount'],
            'forks': repo_data['forkCount'],
            'open_issues': repo_data['openIssues']['totalCount'],
            'closed_issues': repo_data['closedIssues']['totalCount'],
            'subscribers': repo_data['watchers']['totalCount'],
            'commits_last_30_days': len(commits),
            'contributors_count': repo_data['mentionableUsers']['totalCount'],
            'languages': languages,
            'language_percentages': language_percentages,
            'topics': topics,
            'dependencies': dependencies,
            'commit_dates': commits  # For trend analysis
        }

    async def process_batch_of_repos(self, batch_size=10):
        ## get repos
        repos = await self.process_repo_queue(batch_size)
        
        if not repos:
            print("No repos to process")
            return

        ## create async tasks and use gather to wait for all requests
        tasks = []
        valid_repos = []  # Track valid repos in same order as tasks
        
        for repo in repos:
            parts = repo['repo_name'].split('/')
            if len(parts) != 2:
                print(f"⚠️  Invalid repo format: {repo['repo_name']}")
                continue

            owner, name = parts
            tasks.append(self.enrich_repo(repo['repo_id'], owner, name))
            valid_repos.append(repo)  # Keep track of valid repos

        if not tasks:
            print("No valid repos to process")
            return

        print(f"🔄 Processing {len(tasks)} repos...")
        results = await asyncio.gather(*tasks, return_exceptions=True)

        enriched_data_list = []
        successful_repo_ids = []
        failed_repo_ids = []  # Track failed repos to mark as processed
        
        # Match results to repos by index
        for i, result in enumerate(results):
            repo = valid_repos[i]  # Get the corresponding repo
            
            if isinstance(result, Exception):
                print(f"❌ Error enriching repo {repo['repo_id']} ({repo['repo_name']}): {result}")
                failed_repo_ids.append(repo['repo_id'])
                continue
            
            if result is None:
                print(f"❌ Repo {repo['repo_id']} ({repo['repo_name']}) returned None - marking as processed")
                failed_repo_ids.append(repo['repo_id'])  # Mark failed repos too
                continue

            try:
                parsed_data = self.parse_repo_data(result)
                enriched_data_list.append({
                    'repo_id': repo['repo_id'],
                    'repo_name': repo['repo_name'],
                    'activity_score': repo.get('activity_count', 0),
                    'parsed_data': parsed_data
                })
                successful_repo_ids.append(repo['repo_id'])
            except Exception as e:
                print(f"❌ Error parsing repo {repo['repo_id']}: {e}")
                failed_repo_ids.append(repo['repo_id'])

        # Mark both successful AND failed repos as processed
        all_processed_ids = successful_repo_ids + failed_repo_ids
        if all_processed_ids:
            await self.db_helper.mark_repos_as_processed(all_processed_ids)
            if failed_repo_ids:
                print(f"⚠️  Marked {len(failed_repo_ids)} failed repos as processed (won't retry)")

        if enriched_data_list:
            await self.db_helper.bulk_save_enriched_repos(enriched_data_list)
            print(f"✅ Successfully processed {len(enriched_data_list)}/{len(repos)} repos")
        else:
            print("⚠️  No repos were successfully enriched")


async def main():
    processor = Processor(MAX_CON=5)  # Max 5 concurrent API calls
    await processor.setup()
    
    BATCH_SIZE = 10
    MAX_BATCHES = None  # Set to a number to limit, or None for unlimited
    
    batch_count = 0
    while True:
        if MAX_BATCHES and batch_count >= MAX_BATCHES:
            print(f"Reached max batches ({MAX_BATCHES})")
            break
            
        print(f"\n{'='*50}")
        print(f"Batch #{batch_count + 1}")
        print(f"{'='*50}")
        
        await processor.process_batch_of_repos(batch_size=BATCH_SIZE)
        
        # Check if there are more repos to process
        repos = await processor.process_repo_queue(1)
        if not repos:
            print("\n✅ No more repos to process!")
            break
        
        batch_count += 1
        await asyncio.sleep(1)  # Small delay between batches
    
    print(f"\n🎉 Finished processing {batch_count} batches")


if __name__ == "__main__":
    asyncio.run(main())



