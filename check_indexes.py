import sqlite3

conn = sqlite3.connect("data/tempest.db")

rows = conn.execute("""
SELECT name, sql
FROM sqlite_master
WHERE type = 'index'
ORDER BY name
""").fetchall()

print("Indexes found:")
for r in rows:
    print(r)