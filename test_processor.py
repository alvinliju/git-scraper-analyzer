# test_processor.py
import asyncio
from processor import Processor
from dotenv import load_dotenv
load_dotenv()
import os

async def test_enrich_repo():
    """Test enriching a single repo via GitHub API"""
    processor = Processor()
    
    # Test with a well-known repo (e.g., facebook/react)
    repo_id = 10270250
    owner = "facebook"
    name = "react"
    
    print(f"🔍 Testing enrich_repo for {owner}/{name}...")
    
    try:
        repo_data = await processor.enrich_repo(repo_id, owner, name)
        
        if repo_data:
            print("✅ Successfully fetched repo data!")
            print(f"   Stars: {repo_data.get('stargazerCount', 'N/A')}")
            print(f"   Forks: {repo_data.get('forkCount', 'N/A')}")
            print(f"   Open Issues: {repo_data.get('openIssues', {}).get('totalCount', 'N/A')}")
            print(f"   Languages: {len(repo_data.get('languages', {}).get('edges', []))} found")
            
            # Test parsing
            parsed = processor.parse_repo_data(repo_data)
            print("\n📊 Parsed Data:")
            print(f"   Stars: {parsed['stars']}")
            print(f"   Forks: {parsed['forks']}")
            print(f"   Open Issues: {parsed['open_issues']}")
            print(f"   Languages: {list(parsed['languages'].keys())}")
            print(f"   Topics: {list(parsed['topics'].keys())[:5]}")  # First 5 topics
            
            return True
        else:
            print("❌ Failed to fetch repo data")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_parse_with_mock_data():
    """Test parsing with mock data structure"""
    processor = Processor()
    
    # Mock repo data structure
    mock_data = {
        'stargazerCount': 1000,
        'forkCount': 500,
        'openIssues': {'totalCount': 50},
        'closedIssues': {'totalCount': 200},
        'watchers': {'totalCount': 300},
        'defaultBranchRef': {
            'target': {
                'history': {
                    'nodes': [
                        {'committedDate': '2025-01-15T10:00:00Z'},
                        {'committedDate': '2025-01-14T10:00:00Z'},
                    ]
                }
            }
        },
        'mentionableUsers': {'totalCount': 25},
        'languages': {
            'edges': [
                {'node': {'name': 'Python'}, 'size': 5000},
                {'node': {'name': 'JavaScript'}, 'size': 3000},
            ]
        },
        'repositoryTopics': {
            'nodes': [
                {'topic': {'name': 'web'}},
                {'topic': {'name': 'api'}},
            ]
        }
    }
    
    print("\n🧪 Testing parse_repo_data with mock data...")
    try:
        parsed = processor.parse_repo_data(mock_data)
        print("✅ Parsing successful!")
        print(f"   Stars: {parsed['stars']}")
        print(f"   Forks: {parsed['forks']}")
        print(f"   Languages: {parsed['languages']}")
        print(f"   Language %: {parsed['language_percentages']}")
        print(f"   Topics: {parsed['topics']}")
        return True
    except Exception as e:
        print(f"❌ Parsing error: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    print("🚀 Testing Processor (no DB calls)\n")
    
    # Test 1: Parse with mock data (no API call needed)
    await test_parse_with_mock_data()
    
    # Test 2: Real API call (requires GITHUB_TOKEN)
    print("\n" + "="*50)
    if os.getenv("GITHUB_TOKEN"):
        await test_enrich_repo()
    else:
        print("⚠️  GITHUB_TOKEN not set, skipping API test")
        print("   Set it with: export GITHUB_TOKEN=your_token")

if __name__ == "__main__":
    import os
    asyncio.run(main())