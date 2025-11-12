import sys
import json
from main import run_github_query  # Import new function
import asyncio
import os

# New discovery method
def run_new_discovery():
    print("Starting new discovery method (date-based queries)...")
    run_github_query()
    print("✓ Discovery complete - repos added to queue")

if __name__ == "__main__":
    run_new_discovery()

