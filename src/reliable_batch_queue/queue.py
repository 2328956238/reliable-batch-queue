"""Durable batch hand-off with worker, claim and session ownership fencing.

SQLite serializes writers; BEGIN IMMEDIATE keeps reclamation, selection and
claiming in a single transaction. Leases follow a soft-expiration policy: an
expired lease can be reclaimed by claim_next, but remains valid until then.
"""

from contextlib import contextmanager
from dataclasses import dataclass, replace
import math
from pathlib import Path
import sqlite3
import threading
import time
from typing import Callable, Iterable, Iterator
from uuid import uuid4


DEFAULT_PRIORITY = ("urgent", "normal", "bulk")


@dataclass(frozen=True)
class Claim:
    """Ownership proof. Keep all three identity fields when acknowledging work."""

    batch_id: str
    worker_id: str
    claim_id: str
    session_id: str
    attempt: int
    category: str


def _identifier(value: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a nonempty string")
    return value


def _duration(value: float, label: str, *, allow_zero: bool = False) -> float:
    value = float(value)
    if not math.isfinite(value) or value < 0 or (value == 0 and not allow_zero):
        raise ValueError(f"{label} must be finite and {'nonnegative' if allow_zero else 'positive'}")
    return value


class BatchQueue:
    """Local SQLite queue. Workers sharing a DB must use the same lease policy.

    A single host owns the database. For remote workers, put a coordinator API
    in front of this library rather than placing the SQLite file on a network
    filesystem. `clock` is injectable for deterministic tests and demos.
    """

    def __init__(
        self,
        db_path: str | Path,
        *,
        lease_seconds: float = 300,
        starvation_seconds: float = 36 * 3600,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if str(db_path) == ":memory:":
            raise ValueError("Use a file path; each operation opens a new connection")
        self.db_path = Path(db_path)
        self.lease_seconds = _duration(lease_seconds, "lease_seconds")
        self.starvation_seconds = _duration(starvation_seconds, "starvation_seconds")
        self._clock = clock
        self._lock = threading.RLock()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self.db_path), timeout=30)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA synchronous=NORMAL")
            with conn:
                yield conn
        finally:
            conn.close()

    def _initialize(self) -> None:
        with self._lock, self._connection() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS batches (
                    batch_id TEXT PRIMARY KEY,
                    category TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL CHECK(status IN ('ready', 'processing', 'retry', 'done')),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    claimed_by TEXT NOT NULL DEFAULT '',
                    claimed_at REAL,
                    next_attempt_at REAL,
                    last_error TEXT NOT NULL DEFAULT '',
                    claim_id TEXT NOT NULL DEFAULT '',
                    claim_session TEXT NOT NULL DEFAULT ''
                );
                CREATE INDEX IF NOT EXISTS idx_batches_next
                    ON batches(status, next_attempt_at, created_at);
            """)

    def enqueue(self, batch_id: str, category: str = "normal") -> bool:
        """Insert once by logical batch ID, including when a prior batch is done."""
        batch_id = _identifier(batch_id, "batch_id")
        category = _identifier(category, "category")
        with self._lock, self._connection() as conn:
            now = self._clock()
            cursor = conn.execute("""
                INSERT OR IGNORE INTO batches
                    (batch_id, category, status, attempts, created_at, updated_at, next_attempt_at)
                VALUES (?, ?, 'ready', 0, ?, ?, ?)
            """, (batch_id, category, now, now, now))
            return cursor.rowcount == 1

    def claim_next(
        self,
        worker_id: str,
        session_id: str,
        *,
        priority_order: Iterable[str] = DEFAULT_PRIORITY,
    ) -> Claim | None:
        """Reclaim expired leases and atomically claim one eligible batch.

        Eligible aged batches come first in FIFO order. Otherwise category
        priority wins, then FIFO. An empty priority_order means strict FIFO.
        """
        worker_id = _identifier(worker_id, "worker_id")
        session_id = _identifier(session_id, "session_id")
        priorities = tuple(_identifier(item, "priority category") for item in priority_order)
        priority_sql = "0 + 0"
        if priorities:
            priority_sql = "CASE category " + " ".join(
                f"WHEN ? THEN {index}" for index in range(len(priorities))
            ) + f" ELSE {len(priorities)} END"
        with self._lock, self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            # Sample time after acquiring the write lock, not before contention.
            now = self._clock()
            stale_before = now - self.lease_seconds
            starvation_before = now - self.starvation_seconds
            conn.execute("""
                UPDATE batches
                   SET status = 'retry', updated_at = ?, next_attempt_at = ?,
                       claimed_by = '', claimed_at = NULL, claim_id = '', claim_session = '',
                       last_error = CASE WHEN last_error = '' THEN 'lease expired' ELSE last_error END
                 WHERE status = 'processing' AND claimed_at IS NOT NULL AND claimed_at < ?
            """, (now, now, stale_before))
            row = conn.execute(f"""
                SELECT * FROM batches
                 WHERE status IN ('ready', 'retry')
                   AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                 ORDER BY
                   CASE WHEN created_at <= ? THEN 0 ELSE 1 END,
                   CASE WHEN created_at <= ? THEN created_at ELSE NULL END ASC,
                   CASE WHEN created_at <= ? THEN rowid ELSE NULL END ASC,
                   {priority_sql}, created_at ASC, rowid ASC
                 LIMIT 1
            """, (now, starvation_before, starvation_before, starvation_before, *priorities)).fetchone()
            if row is None:
                return None
            claim_id = uuid4().hex
            cursor = conn.execute("""
                UPDATE batches
                   SET status = 'processing', attempts = attempts + 1, claimed_by = ?,
                       claimed_at = ?, updated_at = ?, next_attempt_at = NULL,
                       claim_id = ?, claim_session = ?
                 WHERE batch_id = ? AND status IN ('ready', 'retry')
            """, (worker_id, now, now, claim_id, session_id, row["batch_id"]))
            if cursor.rowcount != 1:
                raise RuntimeError("Claim transaction lost its selected row")
            return Claim(row["batch_id"], worker_id, claim_id, session_id,
                         row["attempts"] + 1, row["category"])

    @staticmethod
    def _ownership(claim: Claim) -> tuple[str, str, str, str]:
        if not isinstance(claim, Claim):
            raise TypeError("A Claim object is required")
        return (
            _identifier(claim.batch_id, "batch_id"),
            _identifier(claim.worker_id, "worker_id"),
            _identifier(claim.claim_id, "claim_id"),
            _identifier(claim.session_id, "session_id"),
        )

    def refresh_lease(self, claim: Claim) -> bool:
        """Heartbeat succeeds only while this exact claim still owns the batch."""
        owner = self._ownership(claim)
        with self._lock, self._connection() as conn:
            now = self._clock()
            cursor = conn.execute("""
                UPDATE batches SET claimed_at = ?, updated_at = ?
                 WHERE batch_id = ? AND status = 'processing'
                   AND claimed_by = ? AND claim_id = ? AND claim_session = ?
            """, (now, now, *owner))
            return cursor.rowcount == 1

    def resume_lease(self, claim: Claim, new_session_id: str) -> Claim | None:
        """CAS-rotate a saved claim to a new session, fencing the previous process.

        Callers must securely persist the old Claim to resume it. Only one
        transition from a given old session succeeds, even during racing restarts.
        """
        owner = self._ownership(claim)
        new_session_id = _identifier(new_session_id, "new_session_id")
        if new_session_id == claim.session_id:
            raise ValueError("Resume requires a new session ID")
        with self._lock, self._connection() as conn:
            now = self._clock()
            cursor = conn.execute("""
                UPDATE batches SET claimed_at = ?, updated_at = ?, claim_session = ?
                 WHERE batch_id = ? AND status = 'processing'
                   AND claimed_by = ? AND claim_id = ? AND claim_session = ?
            """, (now, now, new_session_id, *owner))
            return replace(claim, session_id=new_session_id) if cursor.rowcount == 1 else None

    def complete(self, claim: Claim) -> bool:
        """Acknowledge only the caller's current fenced claim."""
        owner = self._ownership(claim)
        with self._lock, self._connection() as conn:
            cursor = conn.execute("""
                UPDATE batches
                   SET status = 'done', updated_at = ?, next_attempt_at = NULL,
                       claimed_by = '', claimed_at = NULL, last_error = '',
                       claim_id = '', claim_session = ''
                 WHERE batch_id = ? AND status = 'processing'
                   AND claimed_by = ? AND claim_id = ? AND claim_session = ?
            """, (self._clock(), *owner))
            return cursor.rowcount == 1

    def fail(self, claim: Claim, error: str, *, retry_delay_seconds: float = 300) -> bool:
        """Release an owned claim for delayed retry; callers choose retry policy."""
        owner = self._ownership(claim)
        delay = _duration(retry_delay_seconds, "retry_delay_seconds", allow_zero=True)
        with self._lock, self._connection() as conn:
            now = self._clock()
            cursor = conn.execute("""
                UPDATE batches
                   SET status = 'retry', updated_at = ?, next_attempt_at = ?,
                       claimed_by = '', claimed_at = NULL, last_error = ?,
                       claim_id = '', claim_session = ''
                 WHERE batch_id = ? AND status = 'processing'
                   AND claimed_by = ? AND claim_id = ? AND claim_session = ?
            """, (now, now + delay, str(error)[:1000], *owner))
            return cursor.rowcount == 1

    def get(self, batch_id: str) -> dict | None:
        with self._connection() as conn:
            row = conn.execute("SELECT * FROM batches WHERE batch_id = ?", (batch_id,)).fetchone()
            return dict(row) if row is not None else None

    def status_counts(self) -> dict[str, int]:
        with self._connection() as conn:
            rows = conn.execute("SELECT status, COUNT(*) AS count FROM batches GROUP BY status").fetchall()
            return {row["status"]: row["count"] for row in rows}
