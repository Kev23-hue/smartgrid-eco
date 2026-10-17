"""Almacén de nonces en SQLite. Persistente: una repetición tras reiniciar el equipo también se rechaza."""
from __future__ import annotations

import sqlite3
import threading


class SqliteNonceStore:
    def __init__(self, path: str = ":memory:"):
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._lock = threading.Lock()
        self._db.execute("CREATE TABLE IF NOT EXISTS nonces (nonce TEXT PRIMARY KEY, expires_at_ms INTEGER NOT NULL)")
        self._db.commit()

    def remember_if_new(self, nonce: str, expires_at_ms: int) -> bool:
        with self._lock:
            cur = self._db.execute("INSERT OR IGNORE INTO nonces VALUES (?,?)", (nonce, expires_at_ms))
            self._db.commit()
            return cur.rowcount == 1

    def purge_before(self, now_ms: int) -> int:
        """Los nonces de comandos ya vencidos no hacen falta: esos comandos se rechazan por vigencia."""
        with self._lock:
            cur = self._db.execute("DELETE FROM nonces WHERE expires_at_ms < ?", (now_ms,))
            self._db.commit()
            return cur.rowcount

    def close(self) -> None:
        self._db.close()
