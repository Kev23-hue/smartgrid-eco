"""Auditoría local de solo inserción, encadenada por hash (SQLite). ADR-007."""
from __future__ import annotations

import hashlib
import json
import sqlite3

GENESIS = "0" * 64


class SqliteAudit:
    def __init__(self, path: str = ":memory:"):
        self._db = sqlite3.connect(path)
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS audit (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "event TEXT NOT NULL, payload TEXT NOT NULL, prev_hash TEXT NOT NULL, hash TEXT NOT NULL)")
        # Solo inserción: se bloquean UPDATE y DELETE.
        for op in ("UPDATE", "DELETE"):
            self._db.execute(
                f"CREATE TRIGGER IF NOT EXISTS no_{op.lower()} BEFORE {op} ON audit "
                "BEGIN SELECT RAISE(ABORT, 'auditoria de solo insercion'); END")
        self._db.commit()

    @staticmethod
    def _hash(prev: str, event: str, payload: str) -> str:
        return hashlib.sha256(f"{prev}|{event}|{payload}".encode()).hexdigest()

    def _last_hash(self) -> str:
        row = self._db.execute("SELECT hash FROM audit ORDER BY id DESC LIMIT 1").fetchone()
        return row[0] if row else GENESIS

    def record(self, event: str, payload: dict) -> None:
        body = json.dumps(payload, sort_keys=True)
        prev = self._last_hash()
        self._db.execute("INSERT INTO audit (event, payload, prev_hash, hash) VALUES (?,?,?,?)",
                         (event, body, prev, self._hash(prev, event, body)))
        self._db.commit()

    def count(self) -> int:
        return self._db.execute("SELECT COUNT(*) FROM audit").fetchone()[0]

    def verify_chain(self) -> bool:
        prev = GENESIS
        for event, payload, p, h in self._db.execute(
                "SELECT event, payload, prev_hash, hash FROM audit ORDER BY id"):
            if p != prev or h != self._hash(prev, event, payload):
                return False
            prev = h
        return True
