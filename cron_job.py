import sys
import json
from main import run_github_query, process_pending_reposv2
import asyncio
import os
import db_helper
import threading
import time

def run_new_discovery():
    print("Starting new discovery method (date-based queries)...")
    run_github_query()
    print("✓ Discovery complete - repos added to queue")

async def run_processor_loop():
    """Run processor continuously"""
    batch_num = 0
    last_discovery_time = 0
    DISCOVERY_INTERVAL = 3600
    while True:
        batch_num += 1
        print(f"\n--- Batch #{batch_num} ---")
        
        try:
            current_time = time.time()
            pending_count = db_helper.get_pending_repos_from_queue(limit=1)
            should_discover = (
                not pending_count or 
                (current_time - last_discovery_time) > DISCOVERY_INTERVAL  # Hour passed
            )
            if should_discover:
                print("No pending repos, running discovery in background...")
                discovery_thread = threading.Thread(target=run_new_discovery, daemon=True)
                discovery_thread.start()
                last_discovery_time = current_time
                print("⏳ Waiting for discovery to populate queue...")
                await asyncio.sleep(30)
                continue
            else:
                print("Found pending repos, skipping discovery")
            await process_pending_reposv2()

        except Exception as e:
            print(f"❌ Error in batch: {e}")
            import traceback
            traceback.print_exc()
        
        print("⏳ Waiting 30 seconds before next batch...")
        await asyncio.sleep(30)

async def main():
    """Run discovery and processing"""
    print("🚀 Starting GitHub Scraper")
    print("=" * 50)
    
    # Check if we have pending repos
    pending_count = db_helper.get_pending_repos_from_queue(limit=1)
    if not pending_count:
        print("No pending repos, running discovery in background...")
        # Run discovery in separate thread so processor can start immediately
        discovery_thread = threading.Thread(target=run_new_discovery, daemon=True)
        discovery_thread.start()
    else:
        print(f"Found pending repos in queue, skipping discovery")
    
    # Start processor immediately (doesn't wait for discovery)
    print("\n⚙️  Processing queue...")
    #process pending repos 
    await run_processor_loop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Shutting down...")

