import requests
import pandas as pd
import json
import time
import psycopg2

import asyncio
import aiohttp
from aiolimiter import AsyncLimiter
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="pwd",
    port=5432,
)
cur = conn.cursor()

GITHUB_TOKEN="REMOVED_TOKEN"
BASE_URL="https://api.github.com/search/repositories"

# step 1: scrape github repo's
def scrape_github_repos_with_page(page: int):
    print("Starting to scrape github repos...")
    url = f"{BASE_URL}?q=stars:>1000&sort=stars&order=desc&per_page=100&page={page}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}"
    }
    response = requests.get(url, headers=headers)
    return response.json()

def save_github_repos(response: any):
    cur.execute("INSERT INTO github_repos (response) VALUES (%s)", (response,))
    conn.commit()
    print("Data saved to github_repos")

def count_github_repos():
    with open('github_repos.json', 'r', encoding='utf-8') as f:
        data = json.load(f) 
    return len(data['items'])

def scrape_all_github_repos():
    all_repos = []
    for page in range(1, 10):
        response = scrape_github_repos_with_page(page)
        repos = response['items']
        all_repos.extend(repos)
        print(f"Page {page} scraped successfully")
    return all_repos

def save_all_github_repos(repos: list):
    with open('github_repos.json', 'w', encoding='utf-8') as f:
        json.dump(repos, f, ensure_ascii=False, indent=4)
    print("Data saved to github_repos")

##stage 2: get repo details unique to the repos
##this is the function that scrapes individual repo details
async def get_repo_details(repo: dict, rate_limiter: AsyncLimiter):


    async with aiohttp.ClientSession() as session:
        async with rate_limiter:
            try:
                print(f"Getting repo details for {repo['name']}")
                issues_url = f"https://api.github.com/repos/{repo['owner']['login']}/{repo['name']}/issues?state=all"
                commits_url = f"https://api.github.com/repos/{repo['owner']['login']}/{repo['name']}/commits"
                contributors_url = f"https://api.github.com/repos/{repo['owner']['login']}/{repo['name']}/contributors"
                
                # Get repo details to fetch actual commit count from participation stats
                repo_stats_url = f"https://api.github.com/repos/{repo['owner']['login']}/{repo['name']}/stats/participation"
                
                headers = {
                    "Authorization": f"Bearer {GITHUB_TOKEN}"
                }
                
                issues_response =  session.get(issues_url, headers=headers, timeout=10)
                commits_response =  session.get(commits_url, headers=headers, timeout=10)
                contributors_response =  session.get(contributors_url, headers=headers, timeout=10)
                
                # Get participation stats (includes weekly commit counts)
                stats_response = session.get(repo_stats_url, headers=headers, timeout=10)
                
                responses = await asyncio.gather(issues_response, commits_response, contributors_response, stats_response)
                issues = await responses[0].json()
                commits = await responses[1].json()
                contributors = await responses[2].json()
                stats = await responses[3].json()
                total_commits = sum( stats.get('all', []))
                parsed_repo = {
                    'name': repo['name'],
                    'link': repo['html_url'],
                    'likes': repo['stargazers_count'],  # Note: 'likes' not 'stars'
                    'forks': repo['forks_count'],
                    'watchers': repo['watchers_count'],
                    'open_issues_count': repo['open_issues_count'],
                    'language': repo.get('language', 'Unknown'),
                    'created_at': repo['created_at'],
                    'maintainers': repo['owner']['login'],  # Note: 'maintainers' not 'owner'
                    'last-commit-date': repo['pushed_at'],  # Note: hyphen not underscore
                    'total_commits_last_year': total_commits
                }
                return parsed_repo
            except Exception as e:
                print(f"Error getting repo details for {repo['name']}: {e}")
                return None 


##now lets save all that info to a seperate .json file
def save_repo_details(parsedRepo: dict):
    cur.execute("INSERT INTO repo_details (response) VALUES (%s)", (parsedRepo,))
    conn.commit()
    print("Data saved to repo_details")

#  def scan_all_repos_for_stats(repos: list):
#     parsedRepo = []
#     parsed_count = 0
#     total_repos = len(repos)
#     for repo in repos:
#         parsed_count += 1
#         print(f"Scanning repo {parsed_count} of {len(repos)}: {repo['name']}")
        
#         try:
#             issues, commits, contributors, total_commits = get_repo_details(repo)
            
#             parsedRepo.append({
#                 'name': repo['name'],
#                 'link': repo['html_url'],
#                 'likes': repo['stargazers_count'],
#                 'forks': repo['forks_count'],  # From original data
#                 'watchers': repo['watchers_count'],  # From original data
#                 'open_issues_count': repo['open_issues_count'],  # From original data - actual count!
#                 'language': repo.get('language', 'Unknown'),  # Primary language
#                 'created_at': repo['created_at'],  # When repo was created
#                 'maintainers': repo['owner']['login'],
#                 'last-commit-date': repo['pushed_at'],
#                 'total_commits_last_year': total_commits,  # Total commits in last 52 weeks from stats API
#                 'recent-commits-fetched': len(commits),  # We only fetch 30 recent commits
#                 'recent-issues-fetched': len(issues),     # We only fetch 30 recent issues
#                 'top-contributors-fetched': len(contributors),  # We only fetch 30 top contributors
#                 'issues': issues,
#                 'commits': commits, 
#                 'contributors': contributors
#             })
            
#             # Small delay to avoid rate limiting
#             time.sleep(1)
            
#         except Exception as e:
#             print(f"  ❌ Error processing {repo['name']}: {str(e)}")
#             continue

#         if parsed_count % 10 == 0:
#             percentage = (parsed_count / len(repos)) * 100
#             print(f"Progress: {parsed_count}/{total_repos} repos processed ({percentage:.1f}%)")
        
#         if parsed_count %10 == 0:
#             print(f"Saving repo details for repo {parsed_count} of {total_repos}")
#             save_repo_details(parsedRepo)
#             print(f"💾 Checkpoint saved! Current data: {len(parsedRepo)} repos")
            
#     save_repo_details(parsedRepo)
#     print(f"All {parsed_count} repo details saved successfully")
#     return parsedRepo


##run repo scans in parallel and see what breaks
async def run_repo_scans_in_parallel(repos: list):
    try:
        with open('repo_details.json', 'r') as f:
            existing = json.load(f)
        if existing is None or not isinstance(existing, list):
            existing = []
        
        scraped_names = {r['name'] for r in existing if r}
        repos = [r for r in repos if r['name'] not in scraped_names]
        print(f"Skipping {len(scraped_names)} already scraped repos")
    except FileNotFoundError:
        pass
       
        rate_limiter = AsyncLimiter(75, 60) 
        tasks = [get_repo_details(repo, rate_limiter) for repo in repos]
        result = await asyncio.gather(*tasks)
        filtered_result = []
        for r in result:
            if r is not None and not isinstance(r, Exception):
                filtered_result.append(r)
        if len(filtered_result) == 0:
            print("All repos already scraped!")
            return []  # Return empty list, not None
        else:
            print(f"Starting fresh: {len(filtered_result)} repos to scrape")
        return filtered_result

async def main():
    # print("Starting to scrape all github repos...")
    # repos = scrape_all_github_repos()
    # save_all_github_repos(repos)
    # print(f"Total repos scraped: {len(repos)}")
    # print("All github repos scraped successfully")
    repos = []
    with open('github_repos.json', 'r', encoding='utf-8') as f:
        repos = json.load(f)
    print(f"Total repos loaded: {len(repos)}")
    print("Starting to scan all repos for stats...")
    parsedRepo = await run_repo_scans_in_parallel(repos)
    with open('repo_details.json', 'w', encoding='utf-8') as f:
        json.dump(parsedRepo, f, ensure_ascii=False, indent=4)
    print("All repos scanned for stats successfully")

if __name__ == "__main__":
    asyncio.run(main())