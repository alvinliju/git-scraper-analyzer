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
                
                
                # Get repo details to fetch actual commit count from participation stats
                repo_stats_url = f"https://api.github.com/repos/{repo['owner']['login']}/{repo['name']}/stats/participation"
                
                headers = {
                    "Authorization": f"Bearer {GITHUB_TOKEN}"
                }
                
                issues_response = await session.get(issues_url, headers=headers, timeout=10)
                commits_response = await session.get(commits_url, headers=headers, timeout=10)
                contributors_response = await session.get(contributors_url, headers=headers, timeout=10)
                
                # Get participation stats (includes weekly commit counts)
                stats_response = session.get(repo_stats_url, headers=headers, timeout=10)
                
                responses = await asyncio.gather(issues_response, commits_response, contributors_response, stats_response)
                stats = await responses[3].json()
                total_commits = sum( stats.get('all', []))
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

if __name__ == "__main__":
    asyncio.run(main())
