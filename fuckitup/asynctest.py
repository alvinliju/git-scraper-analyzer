import asyncio
import time
from time import sleep
async def fetch_data(repoId:str):
    await asyncio.sleep(3)
    print(f"Fetching data for repo {repoId}")
    return {"repoId": repoId, "data": "some data"}

count = 0
async def fetch_data_with_sem(name, semaphore):
    try:
        
        async with semaphore:
            global count
            count += 1
            if count == 25:
                raise Exception("count 5 error occured")
                return 
            print(f"Fetching data for {name}")
            await asyncio.sleep(3)
            return {"name": name, "data": "some data"}
    except Exception as e:
        print(f"Error fetching data for {name}: {e}")
        return {"name": name, "data": "some data", "error": str(e)}

async def main():
    start = time.time()
    semaphore = asyncio.Semaphore(5)
    # tasks = [fetch_data(f"repo{i}") for i in range(10000)]
    # print(f"Tasks: {tasks}")
    # results = await asyncio.gather(*tasks)
    # print(f"Took {time.time() - start:.2f}s")
    tasks = [fetch_data_with_sem(f"repo{i}", semaphore) for i in range(100)]
    print(f"Tasks: {tasks}")
    results = await asyncio.gather(*tasks, return_exceptions=True)
    print(f"Results: {results}")
    print(f"Took {time.time() - start:.2f}s")
asyncio.run(main())