import requests
import pandas as pd
import json
import time
import psycopg2
from dotenv import load_dotenv
import asyncio
import aiohttp
from aiolimiter import AsyncLimiter
import os
from datetime import datetime, timedelta
from parse_store_in_postgres import parse_and_store_in_postgres

import db_helper

conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="pwd",
    port=5432,
)
cur = conn.cursor()

load_dotenv()

GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
BASE_URL="https://api.github.com/search/repositories"

if not GITHUB_TOKEN:
    raise ValueError("GITHUB_TOKEN not found in environment variables")

# step 1: scrape github repo's
def scrape_github_repos_with_page(page: int):
    print("Starting to scrape github repos...")
    url = f"{BASE_URL}?q=stars:>1000&sort=stars&order=desc&per_page=100&page={page}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}"
    }
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error on page {page}: {response.status_code}")
        return None
    if 'items' not in response.json():
        print(f"No 'items' in response for page {page}")
        return None
    if not response.json()['items']:
        print(f"Page {page} returned no results")
        return None
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
    db_helper.save_raw_repos_to_db(repos)

##stage 2: get repo details unique to the repos
##this is the function that scrapes individual repo details
async def get_repo_details(repo: dict, rate_limiter: AsyncLimiter):
    async with aiohttp.ClientSession() as session:
        async with rate_limiter:
            try:
                print(f"Getting repo details for {repo['name']}")
                
                owner = repo['owner']['login']
                repo_name = repo['name']
                
                # Only fetch stats (the only thing we actually use)
                repo_stats_url = f"https://api.github.com/repos/{owner}/{repo_name}/stats/participation"
                
                headers = {
                    "Authorization": f"Bearer {GITHUB_TOKEN}"
                }
                
                # FIX: Use async context manager properly - this is the correct way!
                async with session.get(repo_stats_url, headers=headers, timeout=10) as stats_response:
                    if stats_response.status != 200:
                        print(f"  ⚠️  Stats API returned {stats_response.status} for {repo['name']}")
                        total_commits = 0
                    else:
                        stats = await stats_response.json()
                        total_commits = sum(stats.get('all', []))
                
                parsed_repo = {
                    'name': repo['name'],
                    'link': repo['html_url'],
                    'likes': repo['stargazers_count'], 
                    'forks': repo['forks_count'],
                    'watchers': repo['watchers_count'],
                    'open_issues_count': repo['open_issues_count'],
                    'language': repo.get('language', 'Unknown'),
                    'created_at': repo['created_at'],
                    'maintainers': repo['owner']['login'], 
                    'last-commit-date': repo['pushed_at'],  
                    'total_commits_last_year': total_commits
                }
                return parsed_repo
            except Exception as e:
                print(f"Error getting repo details for {repo['name']}: {e}")
                import traceback
                traceback.print_exc()
                return None 


##now lets save all that info to a seperate .json file
def save_repo_details(parsedRepo: dict):
    cur.execute("INSERT INTO repo_details (response) VALUES (%s)", (parsedRepo,))
    conn.commit()
    print("Data saved to repo_details")    


##run repo scans in parallel and see what breaks
async def run_repo_scans_in_parallel(repos: list):
        scraped_names = db_helper.get_scraped_repos_name()
        repos = [r for r in repos if r['name'] not in scraped_names]
        print(f"Skipping {len(scraped_names)} already scraped repos")
    
        if not repos:
            print("All repos already scraped!")
            return []
       
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
    print("Starting to scrape all github repos...")
    repos = scrape_all_github_repos()
    save_all_github_repos(repos)
    print(f"Total repos scraped: {len(repos)}")
    
    
    print("Starting to scan all repos for stats...")
    parsedRepo = await run_repo_scans_in_parallel(repos)
    
    if parsedRepo:
        from parse_store_in_postgres import parse_and_store_in_postgres
        parse_and_store_in_postgres(parsedRepo)
    
    print("All repos scanned for stats successfully")


##i am done with life but still we need to scale
##we could generate_querys()
##we need 2 functions
"""
1.first one would be to generate the query
2.sencond one would be to run query and save the repo id to the a set and to the db to keep the state
3.third step would be to poll the db in every 10 mins get a few id's fetch details and mark as fetched_details=false to the db
"""


def generate_github_query():
    queries = []
    
    start = datetime(2024, 1, 1)
    end = datetime.now()
    
    current = start
    while current < end:
        # Use 1-day intervals instead of 10-day to avoid hitting 1000 limit
        next_date = current + timedelta(days=1)
        if next_date > end:
            next_date = end
        
        # Add filters to get more specific results
        query = f"{BASE_URL}?q=created:{current.strftime('%Y-%m-%d')}..{next_date.strftime('%Y-%m-%d')}+stars:>0&sort=created&order=desc&per_page=100"
        queries.append(query)
        current = next_date
    
    return queries


def run_github_query():
    repos = []
    queries = generate_github_query()
    
    print(f"Generated {len(queries)} queries to run...")
    
    for i, url in enumerate(queries, 1):
        headers = {
            "Authorization": f"Bearer {GITHUB_TOKEN}"
        }
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print(f"Error on query {i}/{len(queries)}: {response.status_code}")
            continue
            
        response_data = response.json()  # Parse once
        if 'items' not in response_data:
            print(f"No 'items' in response for query {i}/{len(queries)}")
            continue
            
        if not response_data['items']:
            print(f"No results for query {i}/{len(queries)}")
            continue

        # Collect repos
        repos.extend(response_data['items'])
        print(f"Query {i}/{len(queries)}: Found {len(response_data['items'])} repos (total collected: {len(repos)})")
        
        # Save to queue every 10 repos
        if len(repos) >= 10:
            db_helper.save_to_queue(repos[:10])
            repos = repos[10:]  # Remove saved repos
            print(f"  → Saved 10 repos to queue...")
    
    # Save any remaining repos
    if repos:
        db_helper.save_to_queue(repos)
        print(f"  → Saved remaining {len(repos)} repos to queue")
    
    print("Finished running queries and saving to queue")




async def process_pending_reposv2():
    pending_repos = db_helper.get_pending_repos_from_queue()
    
    if not pending_repos:
        print("No pending repos to process")
        return
    
    print(f"Processing {len(pending_repos)} repos...")
    
    process_repo = []
    repo_id_map = {}
    successful_fetches = []  # Track successful GitHub fetches
    
    for repo_id, repo_full_name in pending_repos:
        try:
            db_helper.mark_repo_in_progress(repo_id)
            
            owner, repo_name = repo_full_name.split('/')
            url = f"https://api.github.com/repos/{owner}/{repo_name}"
            headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                repo_data = response.json()
                process_repo.append(repo_data)
                github_repo_id = repo_data.get('id')
                repo_id_map[github_repo_id] = repo_id
                successful_fetches.append((repo_id, github_repo_id))
                print(f"  ✓ Fetched {repo_full_name}")
            else:
                print(f"  ✗ Failed {repo_full_name}: HTTP {response.status_code}")
                db_helper.mark_repo_failed(repo_id, f"HTTP {response.status_code}")
        except Exception as e:
            print(f"  ✗ Exception for {repo_full_name}: {e}")
            db_helper.mark_repo_failed(repo_id, str(e))
            continue

    if not process_repo:
        print("⚠️  No valid repos fetched - marking all as failed")
        for repo_id, _ in pending_repos:
            db_helper.mark_repo_failed(repo_id, "Failed to fetch from GitHub")
        return
    
    print(f"🔄 Fetching detailed info for {len(process_repo)} repos...")
    
    # This might return empty if repos are already in repos table
    repo_details = await run_repo_scans_in_parallel(process_repo)
    
    print(f"🔍 [DEBUG] run_repo_scans_in_parallel returned {len(repo_details) if repo_details else 0} repos")
    
    if repo_details:
        print(f"✅ Got {len(repo_details)} repo details")
        parse_and_store_in_postgres(repo_details)
        print(f"✅ Saved to database")
        
        # Mark repos that were returned as completed
        completed_count = 0
        for detail in repo_details:
            github_repo_id = detail.get('id')
            if github_repo_id in repo_id_map:
                db_helper.mark_repo_completed(repo_id_map[github_repo_id])
                completed_count += 1
        
        print(f"✅ Marked {completed_count} repos as completed")
    else:
        print("⚠️  No details returned from run_repo_scans_in_parallel")
        print("   This could mean:")
        print("   1. All repos were filtered out (already in repos table)")
        print("   2. get_repo_details() failed for all repos")
        print("   3. Rate limiting issues")
        
        # FIX: Mark ALL successfully fetched repos as completed
        # (Even if they were filtered out, they're already processed!)
        print(f"   Marking {len(successful_fetches)} successfully fetched repos as completed...")
        for repo_id, github_repo_id in successful_fetches:
            db_helper.mark_repo_completed(repo_id)
        
        print(f"✅ Marked {len(successful_fetches)} repos as completed")

if __name__ == "__main__":
    asyncio.run(process_pending_reposv2())
