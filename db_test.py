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

cur.execute(
    """
    CREATE TABLE IF NOT EXISTS repos(
     id SERIAL PRIMARY KEY,
     link TEXT,
     name TEXT,
     stars INT,
     data JSONB,
     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
     """
)

conn.commit()