import sys
import json
from main import scrape_all_github_repos, save_all_github_repos, run_repo_scans_in_parallel
from parse_store_in_postgres import parse_and_store_in_postgres
import asyncio
import os
#scrape
async def run_scraper():  
    repos = scrape_all_github_repos()
    save_all_github_repos(repos)
    print(f"✓ Found {len(repos)} repos")
    details  = await run_repo_scans_in_parallel(repos)

    # Save to JSON
    with open('repo_details.json', 'w') as f:
        json.dump(details, f, indent=2)
    print(f"✓ Scraped {len(details)} repo details")

    #we have constraints in db so we wont save duplicate data
    parse_and_store_in_postgres(details)
    print("✓ Data loaded to Postgres")
    print("🎉 Complete!")

if __name__ == "__main__":
    asyncio.run(run_scraper())

