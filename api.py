import http.server
import socketserver

from flask import Flask, request, jsonify
import psycopg2

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

@app.route('/')
def home():
    return "Hello, World!"


@app.route('/repos', methods=['GET'])
def get_all_repos():
    repos = fetch_all_repos()
    return jsonify(repos)
    



if __name__ == "__main__":
    app.run(debug=True, port=3000)