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

    async def fetch_url_and_download(self, url):
        filename = url.split('/')[-1]
        for attempt in range(3):
            try:
                async with self.semaphore:
                    async with aiohttp.ClientSession() as session:
                        print(f"[Semaphore acquired] Downloading {url}")
                        async with session.get(url) as resp:
                            if resp.status != 200:
                                print(f"✗ {filename}: {resp.status}")
                                return
                            compressed_data = await resp.read()
                            decompressed_data = gzip.decompress(compressed_data).decode('utf-8')
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
                        await self.db_helper.mark_url_done(url)
                        print(f"  Found {len(repo_activity)} repos")

                        return len(repo_activity)
            except Exception as e:
                print(f"Error downloading {url}: {e}")
                await asyncio.sleep(2 * attempt)
        return None




async def main():
    discovery = Discovery()
    await discovery.setup()
    pending_count = len(await discovery.db_helper.get_pending_urls(limit=1))
    if pending_count == 0:
        print("📝 No URLs in queue, computing and inserting...")
        urls = await discovery.compute_gh_archive_url()
        await discovery.db_helper.bulk_insert_urls(urls)
        print(f"✅ Inserted {len(urls)} URLs into queue")
    

    BATCH_SIZE = 100
    while True:
        pending_urls = await discovery.db_helper.get_pending_urls(limit=BATCH_SIZE)
        if not pending_urls:
            break
        tasks = [discovery.fetch_url_and_download(url) for url in pending_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        print(f"Results: {results}")
        print(f"✅ Processed {len(results)} URLs")
        print(f"✅ Pending URLs: {len(await discovery.db_helper.get_pending_urls(limit=1))}")
        print(f"✅ Total URLs: {len(await discovery.db_helper.get_pending_urls())}")
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
