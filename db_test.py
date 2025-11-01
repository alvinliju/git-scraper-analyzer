import psycopg2
import json

conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="pwd",
    port=5432,
)

cur = conn.cursor()

def create_table(): 
    try:
        cur.execute(
            """
            CREATE TABLE repos (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            link VARCHAR(500),
            owner VARCHAR(255),
            stars INTEGER,
            forks INTEGER,
            watchers INTEGER,
            open_issues INTEGER,
            language VARCHAR(100),
            created_at TIMESTAMP,
            last_commit_date TIMESTAMP,
            commits_last_year INTEGER,
            contributors_count INTEGER,
            repo_age_days INTEGER,
            days_since_last_commit INTEGER,
            is_active BOOLEAN,
            is_healthy BOOLEAN,
            scraped_at TIMESTAMP DEFAULT NOW()
            """
        )
        conn.commit()
        print("Table created successfully")
    except Exception as e:
        print(f"Error creating table: {e}")
        conn.rollback()



if __name__ == "__main__":
    print("Starting to create table in postgres...")
    create_table()
    print("Table created successfully")