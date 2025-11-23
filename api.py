
from flask import Flask, request, jsonify
from helpers.db_helper import DBHelper
from helpers.query_engine import execute_sql_query_cached_sync
import asyncio
import threading

app = Flask(__name__)

# Global DB helper instance
db_helper = None
loop = None
loop_thread = None

def run_event_loop(loop):
    """Run event loop in background thread"""
    asyncio.set_event_loop(loop)
    loop.run_forever()

def init_db():
    """Initialize database connection with persistent event loop"""
    global db_helper, loop, loop_thread
    
    # Create event loop in background thread
    loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=run_event_loop, args=(loop,), daemon=True)
    loop_thread.start()
    
    # Create DB helper
    db_helper = DBHelper()
    
    # Run connect in the background loop
    future = asyncio.run_coroutine_threadsafe(db_helper.connect(), loop)
    future.result(timeout=10)  # Wait for connection
    
    print("✅ Database connected for API")


def run_async(coro):
    """Run async coroutine in the background event loop"""
    if loop is None or loop.is_closed():
        raise RuntimeError("Event loop not initialized")
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=30)  # 30 second timeout


@app.route('/')
def home():
    return jsonify({
        "message": "GitHub Repository Query API",
        "endpoints": {
            "/repos": "GET - Get all repos",
            "/repos/<repo_id>": "GET - Get repo by ID",
            "/stats": "GET - Get database statistics",
            "/query": "POST - Natural language query"
        }
    })


@app.route('/repos', methods=['GET'])
def get_all_repos():
    """Get all repos with optional limit"""
    limit = request.args.get('limit', 100, type=int)
    repos = run_async(db_helper.get_all_repos(limit=limit))
    return jsonify(repos)


@app.route('/repos/<int:repo_id>', methods=['GET'])
def get_repo_by_id(repo_id):
    """Get a single repo by repo_id"""
    repo = run_async(db_helper.get_repo_by_id(repo_id))
    if repo:
        return jsonify(repo)
    else:
        return jsonify({"error": "Repo not found"}), 404


@app.route('/stats', methods=['GET'])
def get_stats():
    """Get database statistics"""
    stats = run_async(db_helper.get_stats())
    return jsonify(stats)


@app.route('/query', methods=['POST'])
def natural_language_query():
    """Natural language query endpoint"""
    data = request.json
    question = data.get('question')
    
    if not question:
        return jsonify({"error": "Question is required"}), 400
    
    try:
        limit = data.get('limit', 100)
        # This already handles async internally
        columns, results = execute_sql_query_cached_sync(question, db_helper, limit)
        
        return jsonify({
            "columns": columns,
            "results": results,
            "count": len(results)
        })
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    init_db()
    app.run(debug=True, port=3000)
