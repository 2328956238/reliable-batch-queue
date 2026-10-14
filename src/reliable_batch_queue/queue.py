"""SQLite batch queue with fenced leases, bounded retries and atomic audit.

Leases expire softly: an expired owner remains valid until a claimant recovers
the lease. All mutations and their audit events commit in one transaction.
"""

from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
import json
import math
from pathlib import Path
import sqlite3
import threading
import time
from typing import Callable, Iterable, Iterator
from uuid import uuid4


DEFAULT_PRIORITY = ("urgent", "normal", "bulk")
SCHEMA_VERSION = 2


@dataclass(frozen=True)
class Claim:
    """Ownership proof; attempt is the lifetime claim count, across redrives."""

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


def _integer(value: int, label: str, *, minimum: int = 1, maximum: int = 2**31 - 1) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{label} must be an integer in [{minimum}, {maximum}]")
    return value


@dataclass(frozen=True)
class RetryPolicy:
    """Persisted per batch. max_attempts includes the initial claim."""

    max_attempts: int = 5
    base_delay_seconds: float = 300
    max_delay_seconds: float = 3600

    def __post_init__(self) -> None:
        _integer(self.max_attempts, "max_attempts")
        base = _duration(self.base_delay_seconds, "base_delay_seconds", allow_zero=True)
        cap = _duration(self.max_delay_seconds, "max_delay_seconds", allow_zero=True)
        if cap < base:
            raise ValueError("max_delay_seconds must be >= base_delay_seconds")
        object.__setattr__(self, "base_delay_seconds", base)
        object.__setattr__(self, "max_delay_seconds", cap)

    def delay(self, cycle_attempt: int) -> float:
        """Capped exponential delay without constructing enormous powers."""
        _integer(cycle_attempt, "cycle_attempt")
        if self.base_delay_seconds == 0:
            return 0.0
        exponent = cycle_attempt - 1
        # Subtract logs instead of dividing: cap/base itself could overflow.
        if exponent >= math.log2(self.max_delay_seconds) - math.log2(self.base_delay_seconds):
            return self.max_delay_seconds
        return min(self.max_delay_seconds, math.ldexp(self.base_delay_seconds, exponent))


class BatchQueue:
    """Local SQLite queue; one host owns the DB and a shared lease policy.

    Retry defaults apply only to newly enqueued batches. Each batch stores its
    own policy, so restarting workers with different defaults cannot reset it.
    """

    def __init__(
        self,
        db_path: str | Path,
        *,
        lease_seconds: float = 300,
        starvation_seconds: float = 36 * 3600,
        retry_policy: RetryPolicy | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if str(db_path) == ":memory:":
            raise ValueError("Use a file path; each operation opens a new connection")
        self.db_path = Path(db_path)
        self.lease_seconds = _duration(lease_seconds, "lease_seconds")
        self.starvation_seconds = _duration(starvation_seconds, "starvation_seconds")
        self.retry_policy = self._policy(retry_policy)
        self._clock = clock
        self._lock = threading.RLock()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @staticmethod
    def _policy(policy: RetryPolicy | None) -> RetryPolicy:
        if policy is None:
            return RetryPolicy()
        if not isinstance(policy, RetryPolicy):
            raise TypeError("retry_policy must be a RetryPolicy")
        return policy

    @staticmethod
    def _row_policy(row: sqlite3.Row) -> RetryPolicy:
        return RetryPolicy(row["max_attempts"], row["retry_base_seconds"], row["retry_max_seconds"])

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

    @staticmethod
    def _create_batches(conn: sqlite3.Connection) -> None:
        conn.execute("""
            CREATE TABLE batches (
                batch_id TEXT PRIMARY KEY,
                category TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL CHECK(status IN ('ready', 'processing', 'retry', 'done', 'dead_letter')),
                attempts INTEGER NOT NULL DEFAULT 0,
                cycle_attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL CHECK(max_attempts > 0),
                retry_base_seconds REAL NOT NULL CHECK(retry_base_seconds >= 0),
                retry_max_seconds REAL NOT NULL CHECK(retry_max_seconds >= retry_base_seconds),
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                claimed_by TEXT NOT NULL DEFAULT '',
                claimed_at REAL,
                next_attempt_at REAL,
                last_error TEXT NOT NULL DEFAULT '',
                claim_id TEXT NOT NULL DEFAULT '',
                claim_session TEXT NOT NULL DEFAULT ''
            )
        """)

    def _initialize(self) -> None:
        with self._lock, self._connection() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("BEGIN IMMEDIATE")
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            if version > SCHEMA_VERSION:
                raise ValueError(f"Unsupported future schema version: {version}")
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(batches)")}
            conn.execute("""
                CREATE TABLE IF NOT EXISTS audit_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    occurred_at REAL NOT NULL,
                    actor TEXT NOT NULL,
                    reason TEXT NOT NULL DEFAULT '',
                    attempt INTEGER NOT NULL,
                    cycle_attempt INTEGER NOT NULL,
                    claim_id TEXT NOT NULL DEFAULT '',
                    session_id TEXT NOT NULL DEFAULT '',
                    details TEXT NOT NULL
                )
            """)
            if not columns:
                self._create_batches(conn)
            elif "max_attempts" not in columns:
                # SQLite cannot widen a CHECK constraint with ALTER COLUMN.
                # The table rebuild and its audit baseline are one transaction.
                conn.execute("ALTER TABLE batches RENAME TO batches_legacy")
                self._create_batches(conn)
                policy = self.retry_policy
                conn.execute("""
                    INSERT INTO batches
                        (rowid, batch_id, category, status, attempts, cycle_attempts,
                         max_attempts, retry_base_seconds, retry_max_seconds, created_at,
                         updated_at, claimed_by, claimed_at, next_attempt_at, last_error,
                         claim_id, claim_session)
                    SELECT rowid, batch_id, category, status, attempts, attempts,
                           MAX(?, attempts + 1), ?, ?, created_at, updated_at, claimed_by,
                           claimed_at, next_attempt_at, last_error, claim_id, claim_session
                      FROM batches_legacy ORDER BY rowid
                """, (policy.max_attempts, policy.base_delay_seconds, policy.max_delay_seconds))
                now = self._clock()
                for row in conn.execute("SELECT * FROM batches ORDER BY rowid").fetchall():
                    self._event(conn, row, "legacy_imported", now, "schema-migration",
                                "Pre-migration event history is unavailable",
                                {"status": row["status"], "policy": asdict(self._row_policy(row))})
                conn.execute("DROP TABLE batches_legacy")
            elif not {"cycle_attempts", "retry_base_seconds", "retry_max_seconds"} <= columns:
                raise ValueError("Incomplete retry schema; restore a known database backup")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_batches_next ON batches(status, next_attempt_at, created_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_batch ON audit_events(batch_id, event_id)")
            conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    @staticmethod
    def _event(conn: sqlite3.Connection, row: sqlite3.Row, event_type: str,
               now: float, actor: str, reason: str = "", details: dict | None = None) -> None:
        conn.execute("""
            INSERT INTO audit_events
                (batch_id, event_type, occurred_at, actor, reason, attempt, cycle_attempt,
                 claim_id, session_id, details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (row["batch_id"], event_type, now, actor, reason[:1000], row["attempts"],
              row["cycle_attempts"], row["claim_id"], row["claim_session"],
              json.dumps(details or {}, ensure_ascii=False, allow_nan=False)))

    def enqueue(self, batch_id: str, category: str = "normal", *,
                retry_policy: RetryPolicy | None = None) -> bool:
        """Insert once; neither duplicate enqueue nor reopen changes the policy."""
        batch_id = _identifier(batch_id, "batch_id")
        category = _identifier(category, "category")
        policy = self.retry_policy if retry_policy is None else self._policy(retry_policy)
        with self._lock, self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = self._clock()
            cursor = conn.execute("""
                INSERT OR IGNORE INTO batches
                    (batch_id, category, status, created_at, updated_at, next_attempt_at,
                     max_attempts, retry_base_seconds, retry_max_seconds)
                VALUES (?, ?, 'ready', ?, ?, ?, ?, ?, ?)
            """, (batch_id, category, now, now, now, policy.max_attempts,
                  policy.base_delay_seconds, policy.max_delay_seconds))
            if cursor.rowcount != 1:
                return False
            row = conn.execute("SELECT * FROM batches WHERE batch_id = ?", (batch_id,)).fetchone()
            self._event(conn, row, "enqueued", now, "producer", details={"policy": asdict(policy)})
            return True

    def _release_failed(self, conn: sqlite3.Connection, row: sqlite3.Row, now: float,
                        actor: str, error: str, cause: str, delay_override: float | None = None) -> None:
        exhausted = row["cycle_attempts"] >= row["max_attempts"]
        status = "dead_letter" if exhausted else "retry"
        delay = None if exhausted else (delay_override if delay_override is not None
                                       else self._row_policy(row).delay(row["cycle_attempts"]))
        next_at = None if delay is None else now + delay
        conn.execute("""
            UPDATE batches
               SET status = ?, updated_at = ?, next_attempt_at = ?,
                   claimed_by = '', claimed_at = NULL, last_error = ?,
                   claim_id = '', claim_session = ''
             WHERE batch_id = ?
        """, (status, now, next_at, str(error)[:1000], row["batch_id"]))
        self._event(conn, row, "dead_lettered" if exhausted else "retry_scheduled", now,
                    actor, str(error), {"cause": cause, "from_status": "processing", "to_status": status,
                                       "next_attempt_at": next_at, "delay_seconds": delay,
                                       "delay_override": delay_override is not None})

    def claim_next(self, worker_id: str, session_id: str, *,
                   priority_order: Iterable[str] = DEFAULT_PRIORITY) -> Claim | None:
        """Reclaim stale leases and claim one eligible batch in one transaction."""
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
            now = self._clock()  # After acquiring the write lock.
            starvation_before = now - self.starvation_seconds
            expired = conn.execute("""
                SELECT * FROM batches WHERE status = 'processing'
                  AND claimed_at IS NOT NULL AND claimed_at < ? ORDER BY rowid
            """, (now - self.lease_seconds,)).fetchall()
            for row in expired:
                self._release_failed(conn, row, now, worker_id, "lease expired", "lease_expired")
            row = conn.execute(f"""
                SELECT * FROM batches
                 WHERE status IN ('ready', 'retry') AND cycle_attempts < max_attempts
                   AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                 ORDER BY
                   CASE WHEN created_at <= ? THEN 0 ELSE 1 END,
                   CASE WHEN created_at <= ? THEN created_at ELSE NULL END ASC,
                   CASE WHEN created_at <= ? THEN rowid ELSE NULL END ASC,
                   {priority_sql}, created_at ASC, rowid ASC LIMIT 1
            """, (now, starvation_before, starvation_before, starvation_before, *priorities)).fetchone()
            if row is None:
                return None
            claim_id = uuid4().hex
            conn.execute("""
                UPDATE batches
                   SET status = 'processing', attempts = attempts + 1, cycle_attempts = cycle_attempts + 1,
                       claimed_by = ?, claimed_at = ?, updated_at = ?, next_attempt_at = NULL,
                       claim_id = ?, claim_session = ? WHERE batch_id = ?
            """, (worker_id, now, now, claim_id, session_id, row["batch_id"]))
            claimed = conn.execute("SELECT * FROM batches WHERE batch_id = ?", (row["batch_id"],)).fetchone()
            self._event(conn, claimed, "claimed", now, worker_id, details={"from_status": row["status"]})
            return Claim(row["batch_id"], worker_id, claim_id, session_id,
                         claimed["attempts"], row["category"])

    @staticmethod
    def _ownership(claim: Claim) -> tuple[str, str, str, str]:
        if not isinstance(claim, Claim):
            raise TypeError("A Claim object is required")
        return (_identifier(claim.batch_id, "batch_id"), _identifier(claim.worker_id, "worker_id"),
                _identifier(claim.claim_id, "claim_id"), _identifier(claim.session_id, "session_id"))

    @staticmethod
    def _owned(conn: sqlite3.Connection, owner: tuple[str, str, str, str]) -> sqlite3.Row | None:
        return conn.execute("""
            SELECT * FROM batches WHERE batch_id = ? AND status = 'processing'
              AND claimed_by = ? AND claim_id = ? AND claim_session = ?
        """, owner).fetchone()

    def refresh_lease(self, claim: Claim) -> bool:
        """Heartbeat succeeds only while the exact Claim still owns the batch."""
        owner = self._ownership(claim)
        with self._lock, self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = self._owned(conn, owner)
            if row is None:
                return False
            now = self._clock()
            conn.execute("UPDATE batches SET claimed_at = ?, updated_at = ? WHERE batch_id = ?",
                         (now, now, claim.batch_id))
            self._event(conn, row, "heartbeat", now, claim.worker_id)
            return True

    def resume_lease(self, claim: Claim, new_session_id: str) -> Claim | None:
        """CAS-rotate a saved Claim; a racing old session cannot rotate twice."""
        owner = self._ownership(claim)
        new_session_id = _identifier(new_session_id, "new_session_id")
        if new_session_id == claim.session_id:
            raise ValueError("Resume requires a new session ID")
        with self._lock, self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = self._owned(conn, owner)
            if row is None:
                return None
            now = self._clock()
            conn.execute("""UPDATE batches SET claimed_at = ?, updated_at = ?, claim_session = ?
                            WHERE batch_id = ?""", (now, now, new_session_id, claim.batch_id))
            self._event(conn, row, "session_rotated", now, claim.worker_id,
                        details={"new_session_id": new_session_id})
            return replace(claim, session_id=new_session_id)

    def complete(self, claim: Claim) -> bool:
        """Acknowledge only the caller's current fenced Claim."""
        owner = self._ownership(claim)
        with self._lock, self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = self._owned(conn, owner)
            if row is None:
                return False
            now = self._clock()
            conn.execute("""
                UPDATE batches SET status = 'done', updated_at = ?, next_attempt_at = NULL,
                    claimed_by = '', claimed_at = NULL, last_error = '', claim_id = '', claim_session = ''
                 WHERE batch_id = ?
            """, (now, claim.batch_id))
            self._event(conn, row, "completed", now, claim.worker_id)
            return True

    def fail(self, claim: Claim, error: str, *, retry_delay_seconds: float | None = None) -> bool:
        """Apply persisted backoff; an explicit delay retains the legacy override.

        The override changes only this retry's timing, never the attempt budget.
        """
        owner = self._ownership(claim)
        delay = None if retry_delay_seconds is None else _duration(
            retry_delay_seconds, "retry_delay_seconds", allow_zero=True)
        with self._lock, self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = self._owned(conn, owner)
            if row is None:
                return False
            self._release_failed(conn, row, self._clock(), claim.worker_id, error, "explicit_failure", delay)
            return True

    def redrive(self, batch_id: str, *, actor: str, reason: str) -> bool:
        """Manually recover dead-letter work without erasing previous attempts.

        One racing caller wins. A new cycle keeps the persisted retry policy and
        original enqueue time; authorization belongs in the calling service.
        """
        batch_id = _identifier(batch_id, "batch_id")
        actor = _identifier(actor, "actor")
        reason = _identifier(reason, "reason")
        with self._lock, self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM batches WHERE batch_id = ? AND status = 'dead_letter'",
                               (batch_id,)).fetchone()
            if row is None:
                return False
            now = self._clock()
            conn.execute("""UPDATE batches SET status = 'ready', cycle_attempts = 0,
                            next_attempt_at = ?, updated_at = ? WHERE batch_id = ?""",
                         (now, now, batch_id))
            self._event(conn, row, "redriven", now, actor, reason,
                        {"from_status": "dead_letter", "to_status": "ready", "new_cycle_attempts": 0})
            return True

    def get(self, batch_id: str) -> dict | None:
        with self._connection() as conn:
            row = conn.execute("SELECT * FROM batches WHERE batch_id = ?", (batch_id,)).fetchone()
            return dict(row) if row is not None else None

    def status_counts(self) -> dict[str, int]:
        with self._connection() as conn:
            rows = conn.execute("SELECT status, COUNT(*) AS count FROM batches GROUP BY status").fetchall()
            return {row["status"]: row["count"] for row in rows}

    def events(self, batch_id: str | None = None, *, after_event_id: int = 0, limit: int = 100) -> list[dict]:
        """Read ordered audit events using a global, stable event ID cursor."""
        _integer(after_event_id, "after_event_id", minimum=0, maximum=2**63 - 1)
        _integer(limit, "limit", maximum=1000)
        if batch_id is not None:
            _identifier(batch_id, "batch_id")
        with self._connection() as conn:
            rows = conn.execute("""
                SELECT * FROM audit_events WHERE event_id > ? AND (? IS NULL OR batch_id = ?)
                 ORDER BY event_id LIMIT ?
            """, (after_event_id, batch_id, batch_id, limit)).fetchall()
            return [dict(row) | {"details": json.loads(row["details"])} for row in rows]
