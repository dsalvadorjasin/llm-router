"""Seed conversations with N messages directly into an APP_DB_PATH sqlite file.

    uv run python bench/experiments/seed_history.py DB_PATH MESSAGES [COUNT] [CHARS]
Prints the conversation ids. Run while the router is stopped (or before it starts).
"""
import sys

from app.store import Store

path, n = sys.argv[1], int(sys.argv[2])
count = int(sys.argv[3]) if len(sys.argv) > 3 else 1
chars = int(sys.argv[4]) if len(sys.argv) > 4 else 400
store = Store(path)
body = ("lorem ipsum dolor sit amet " * (chars // 27 + 1))[:chars]
for _ in range(count):
    conv = store.create_conversation("seeded")
    rows = [(f"{conv['id']}-{i}", conv["id"], "user" if i % 2 == 0 else "assistant", body, None, float(i))
            for i in range(n)]
    store._conn.executemany(
        "INSERT INTO messages (id, conversation_id, role, content, latency_ms, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?)", rows)
    store._conn.commit()
    print(conv["id"])
store.close()
