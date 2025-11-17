## we need to scrape the github archives
## download all the github arhieve file and parse it and delete it save raw data to the db with postgres queue
## now write a sepreate aggregate.py and turn the raw data into meaningful data and save it the another db 

##currently we can get lot of gh archieve files
##we can build a processor which takes in a file and get some events 


import asyncio
import aiohttp as aiohttp
import os
from datetime import datetime, timedelta
from pathlib import Path
import json
import gzip


import helpers.db_helper as db_helper

class Discovery:
    def __init__(self, MAX_CONCURRENCY=5):
        self.db_helper = db_helper.DBHelper()
        self.base_url = "https://data.gharchive.org"
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENCY)


    ##compute all the gh_archieve links and store it in json and use it as source of truth
    async def compute_gh_archive_url(self):
        start = datetime(2025, 1, 1)
        end = datetime.now()
        current = start
        urls = []

        while current < end:
            for hour in range(24):
                filename = f"{current.strftime('%Y-%m-%d')}-{hour}.json.gz"
                urls.append(f"{self.base_url}/{filename}")
            current += timedelta(days=1)
        return urls

    async def setup(self):
        await self.db_helper.connect()
        print("✅ Database connected")

    ##download_hour takes 
    async def download_hour(self, date, hour):
        filename = f"{date.strftime('%Y-%m-%d')}-{hour}.json.gz"
        url = f"https://data.gharchive.org/{filename}"

        async with self.semaphore:
            async with aiohttp.ClientSession() as session:
                print(f"[Semaphore acquired] Downloading {url}")
                async with session.get(url) as resp:
                    print(f"Response: {resp.status}")
                    
                    if resp.status != 200:
                        print(f"✗ {filename}: {resp.status}")
                        return

                    compressed_data = await resp.read()


            decompressed_data = gzip.decompress(compressed_data).decode('utf-8')

            repo_activity = {}

            for line in decompressed_data.splitlines('\n'):
                if not line:
                    continue

                try:
                    event = json.loads(line)
                    repo_id = event['repo']['id']
                    repo_name = event['repo']['name']
                    if repo_id not in repo_activity:
                        repo_activity[repo_id] = {
                            'name': repo_name,
                            'count': 0
                        }
                    repo_activity[repo_id]['count'] += 1
                except:
                    continue
         
        #TODO:save to db
        await self.db_helper.save_repo_id_to_queue(repo_activity)
        print(f"  Found {len(repo_activity)} repos")

        return len(repo_activity)



async def main():
    discovery = Discovery()
    await discovery.setup()
    urls = await discovery.compute_gh_archive_url()
    print(f"Found {len(urls)} URLs")
    with open('gh_archive_urls.json', 'w') as f:
        json.dump(urls, f)
    # tasks = []
    # begin_date = datetime(2025, 1, 1)
    # end_date = datetime.now()
    # while begin_date < end_date:
    #     for i in range(0, 24):
    #         task = asyncio.create_task(discovery.download_hour(begin_date, i))
    #         tasks.append(task)
    #     results = await asyncio.gather(*tasks, return_exceptions=True)
    #     print(f"Results: {results}")
    #     begin_date += timedelta(days=2)
    #     tasks = []

    print("✅ Database disconnected")
    print("✅ Discovery completed")

if __name__ == "__main__":
    asyncio.run(main())
