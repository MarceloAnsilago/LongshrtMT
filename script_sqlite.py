import sqlite3
conn = sqlite3.connect('db.sqlite3')
cur = conn.cursor()
cur.execute("SELECT name,type FROM sqlite_master WHERE type IN ('table','view') ORDER BY name;")
rows = cur.fetchall()
print('\n'.join(f"{row[1]}: {row[0]}" for row in rows))
cur.close()
conn.close()
