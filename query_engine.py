# query_engine.py
import os 
from dotenv import load_dotenv
import psycopg2
from google import genai

load_dotenv()
gemini_api_key = os.getenv('GEMINI_API_KEY')
client = genai.Client(api_key=gemini_api_key)

SCHEMA_CONTEXT = """
You are a SQL query generator for a GitHub repository database.

Table: repos
Columns:
- id (integer, primary key)
- name (varchar, repo name)
- link (varchar, GitHub URL)
- owner (varchar, repo owner)
- stars (integer, stargazers count)
- forks (integer, fork count)
- watchers (integer)
- open_issues (integer)
- language (varchar, primary programming language)
- created_at (timestamp)
- last_commit_date (timestamp)
- commits_last_year (integer)
- repo_age_days (integer)
- days_since_last_commit (integer)
- is_active (boolean, true if committed in last 30 days)
- is_healthy (boolean, true if active and many commits)

Examples:
Q: "Show me top 10 Python repos"
A: SELECT name, stars, owner FROM repos WHERE language='Python' ORDER BY stars DESC LIMIT 10;

Q: "How many repos are healthy?"
A: SELECT COUNT(*) FROM repos WHERE is_healthy=true;

Q: "Find stale repos with high stars"
A: SELECT name, stars, days_since_last_commit FROM repos WHERE stars > 10000 AND days_since_last_commit > 365 ORDER BY stars DESC;

Generate ONLY the SQL query, no explanations.
"""

def generate_sql_query(prompt:str) -> str:
    response = client.models.generate_content(
        model="gemini-2.5-flash", contents=SCHEMA_CONTEXT + "\n\nQ: " + prompt + "\nA:"
    )
    return response.text


DANGEROUS_KEYWORDS = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'TRUNCATE']

def is_safe_to_execute(query:str) -> bool:
    sql_query = query.upper()

    if "SELECT" not in sql_query:
        return False

    for word in DANGEROUS_KEYWORDS:
        if word in sql_query:
         return False
    

    return True


# def execute_sql_query(query:str):
#     if not is_safe_to_execute(query):
#         raise ValueError("Unsafe to execute")
    
#     conn = psycopg2.connect(
#     host="localhost",
#     database="postgres",
#     user="postgres",
#     password="pwd",
#     port=5432,
#     )

#     cur = conn.cursor()

#     if 'LIMIT' not in sql_query:
#         sql += 'LIMIT 100'

#     cur.execute("SET statement_timeout = 5000")

#     cur.execute(sql_query)
#     results = cur.fetchall()
#     columns = [desc[0] for desc in cur.description]
#     conn.commit()
#     cur.close()
#     conn.close()


#     return columns, results

    
def execute_sql_query(question: str):

    sql_query = generate_sql_query(question)
    print(f"Generated SQL: {sql_query}")  
    

    sql_query = sql_query.replace('', '').replace('```', '').strip()
    

    if not is_safe_to_execute(sql_query):
        raise ValueError(f"Unsafe SQL generated: {sql_query}")
    

    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="pwd",
        port=5432,
    )
    
    cur = conn.cursor()
    

    if 'LIMIT' not in sql_query.upper():
        sql_query += ' LIMIT 100'

    sql_query = sql_query.replace(";", "")
    print(f"Executing SQL: {sql_query}")
    

    cur.execute("SET statement_timeout = 5000")
    

    cur.execute(sql_query)
    results = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    
    conn.close()
    
    return columns, results



