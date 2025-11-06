import http.server
import socketserver

from flask import Flask, request, jsonify
import psycopg2
from query_engine import execute_sql_query_cached

app = Flask(__name__)

##helper functions
def get_db_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="pwd",
        port=5432,
    )
    return conn

def fetch_all_repos():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM repos")
    repos = cur.fetchall()
    conn.close()
    return repos

def fetch_repo_by_id(id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM repos WHERE id = %s", [id])
    repo = cur.fetchone()
    conn.close()
    return repo

def fetch_stats():
    conn = get_db_connection()
    cur = conn.cursor()
    ## we need to return total repo count, healthy count, avg stars, most popular language
    cur.execute("SELECT COUNT(*) FROM repos")
    total_repos = cur.fetchone()
    healthy_repos = cur.execute("SELECT COUNT(*) FROM repos WHERE  is_healthy  = TRUE")
    healthy_repos = cur.fetchone()
    avg_stars = cur.execute("SELECT AVG(stars) FROM repos")
    avg_stars = cur.fetchone()
    most_popular_language = cur.execute("SELECT language FROM repos GROUP BY language ORDER BY COUNT(*) DESC LIMIT 1")
    most_popular_language = cur.fetchone()
    conn.close()
    return {
        "total_repos": total_repos,
        "healthy_repos": healthy_repos,
        "avg_stars": avg_stars,
        "most_popular_language": most_popular_language
    }

@app.route('/')
def home():
    return "Hello, World!"


@app.route('/repos', methods=['GET'])
def get_all_repos():
    repos = fetch_all_repos()
    return jsonify(repos)

@app.route('/repos/<int:id>', methods=['GET'])
def get_repo_by_id(id):
    repo = fetch_repo_by_id(id)
    return jsonify(repo)

@app.route('/stats', methods=['GET'])
def get_stats():
    stats = fetch_stats()
    return jsonify(stats)


@app.route('/query', methods=['POST'])
def natural_language_query():
    data = request.json
    query = data.get('question')
    if not query:
        return jsonify({"error": "Query is required"}), 400
    try:
        result = execute_sql_query_cached(query)
        print(f"Result type: {type(result)}")  # Debug
        print(f"Result: {result}")  # Debug
        columns, results = result  # This line is failing
        return jsonify({"columns": columns, "results": results})
    except Exception as e:
        import traceback
        traceback.print_exc()  # Print full error
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, port=3000)