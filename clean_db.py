import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="pwd",
    port=5432,
)

cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM repos ")
before = cur.fetchone()[0]
print(f"Before: {before}")

cur.execute("SELECT name, COUNT(*) FROM repos GROUP BY name HAVING COUNT(*)>1 ORDER BY count DESC")
duplicates = cur.fetchall()
print(f"Duplicates: {duplicates}")

print("\nDeleting duplicates...")
cur.execute("DELETE FROM repos WHERE id NOT IN(SELECT MIN(id) FROM repos GROUP BY name)")
deleted = cur.rowcount

conn.commit()


# Count after
cur.execute("SELECT COUNT(*) FROM repos")
after = cur.fetchone()[0]

print(f"\n✓ Done!")
print(f"  Before: {before} repos")
print(f"  Deleted: {deleted} duplicates")
print(f"  After: {after} unique repos")

conn.close()