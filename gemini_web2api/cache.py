"""Small SQLite-backed context cache for reusable chat messages."""
import json
import hashlib
import os
import sqlite3
import threading
import time
import uuid
from contextlib import closing

from .config import CONFIG


class CacheStore:
    def __init__(self):
        self._init_lock = threading.Lock()
        self._initialized_path = None
        self._local = threading.local()

    def _connect(self):
        path = CONFIG["cache_db_path"]
        if self._initialized_path != path:
            with self._init_lock:
                if self._initialized_path != path:
                    parent = os.path.dirname(os.path.abspath(path))
                    os.makedirs(parent, exist_ok=True)
                    with closing(sqlite3.connect(path, timeout=10)) as conn:
                        conn.execute("PRAGMA journal_mode=WAL")
                        conn.execute("PRAGMA busy_timeout=10000")
                        conn.execute(
                            "CREATE TABLE IF NOT EXISTS context_cache "
                            "(id TEXT PRIMARY KEY, messages TEXT NOT NULL, created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL)"
                        )
                        conn.commit()
                    self._initialized_path = path
        # Thread-local reuse: avoids connect+PRAGMA per request under load.
        conn = getattr(self._local, "conn", None)
        conn_path = getattr(self._local, "path", None)
        if conn is None or conn_path != path:
            if conn is not None:
                try:
                    conn.close()
                except sqlite3.Error:
                    pass
            conn = sqlite3.connect(path, timeout=10)
            conn.execute("PRAGMA busy_timeout=10000")
            self._local.conn = conn
            self._local.path = path
        return conn

    def create(self, messages: list, ttl_sec: int) -> dict:
        now = int(time.time())
        item = {"id": f"cache_{uuid.uuid4().hex}", "created_at": now, "expires_at": now + ttl_sec}
        conn = self._connect()
        conn.execute(
            "INSERT INTO context_cache (id, messages, created_at, expires_at) VALUES (?, ?, ?, ?)",
            (item["id"], json.dumps(messages, ensure_ascii=False), item["created_at"], item["expires_at"]),
        )
        conn.execute("DELETE FROM context_cache WHERE expires_at <= ?", (now,))
        conn.commit()
        return item

    def get_or_create_auto(self, messages: list, ttl_sec: int, scope: str = "") -> dict:
        """Reuse a deterministic cache entry for an identical system prefix."""
        payload = scope + "\n" + json.dumps(messages, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        cache_id = "cache_auto_" + hashlib.sha256(payload.encode()).hexdigest()[:32]
        cached = self.get(cache_id)
        if cached:
            return cached
        now = int(time.time())
        item = {"id": cache_id, "created_at": now, "expires_at": now + ttl_sec}
        conn = self._connect()
        conn.execute(
            "INSERT INTO context_cache (id, messages, created_at, expires_at) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET messages=excluded.messages, created_at=excluded.created_at, expires_at=excluded.expires_at",
            (cache_id, json.dumps(messages, ensure_ascii=False), now, item["expires_at"]),
        )
        conn.commit()
        row = conn.execute(
            "SELECT messages, created_at, expires_at FROM context_cache WHERE id = ?", (cache_id,)
        ).fetchone()
        return {"id": cache_id, "messages": json.loads(row[0]), "created_at": row[1], "expires_at": row[2]}

    def get(self, cache_id: str):
        now = int(time.time())
        conn = self._connect()
        row = conn.execute(
            "SELECT messages, created_at, expires_at FROM context_cache WHERE id = ?", (cache_id,)
        ).fetchone()
        if not row or row[2] <= now:
            conn.execute("DELETE FROM context_cache WHERE id = ?", (cache_id,))
            conn.commit()
            return None
        return {"id": cache_id, "messages": json.loads(row[0]), "created_at": row[1], "expires_at": row[2]}

    def delete(self, cache_id: str) -> bool:
        conn = self._connect()
        deleted = conn.execute("DELETE FROM context_cache WHERE id = ?", (cache_id,)).rowcount > 0
        conn.commit()
        return deleted


CACHE = CacheStore()
