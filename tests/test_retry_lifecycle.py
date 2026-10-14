from pathlib import Path
import multiprocessing
import sqlite3
from tempfile import TemporaryDirectory
import unittest

from reliable_batch_queue import BatchQueue, RetryPolicy
from test_queue import Clock


def race_operation(db_path, operation, barrier, output):
    queue = BatchQueue(db_path, lease_seconds=30, clock=lambda: 1031.0)
    barrier.wait(timeout=15)
    if operation == "redrive":
        output.put(queue.redrive("batch-a", actor="operator", reason="dependency repaired"))
    else:
        output.put(queue.claim_next("worker", "session"))


LEGACY_SCHEMA = """
CREATE TABLE batches (
    batch_id TEXT PRIMARY KEY, category TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL CHECK(status IN ('ready', 'processing', 'retry', 'done')),
    attempts INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL, updated_at REAL NOT NULL,
    claimed_by TEXT NOT NULL DEFAULT '', claimed_at REAL, next_attempt_at REAL,
    last_error TEXT NOT NULL DEFAULT '', claim_id TEXT NOT NULL DEFAULT '',
    claim_session TEXT NOT NULL DEFAULT ''
);
CREATE INDEX idx_batches_next ON batches(status, next_attempt_at, created_at);
"""


class RetryLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "queue.sqlite3"
        self.clock = Clock()
        self.policy = RetryPolicy(max_attempts=4, base_delay_seconds=10, max_delay_seconds=25)
        self.queue = BatchQueue(self.path, lease_seconds=30, clock=self.clock, retry_policy=self.policy)

    def claim(self):
        claim = self.queue.claim_next("worker-a", "session-a")
        self.assertIsNotNone(claim)
        return claim

    def test_exponential_backoff_cap_budget_and_exact_due_boundary(self):
        self.queue.enqueue("batch-a")
        for attempt, delay in enumerate((10, 20, 25), start=1):
            claim = self.claim()
            self.assertEqual(claim.attempt, attempt)
            self.assertTrue(self.queue.fail(claim, "dependency unavailable"))
            self.assertEqual(self.queue.get("batch-a")["next_attempt_at"], self.clock.now + delay)
            self.clock.advance(delay - 0.01)
            self.assertIsNone(self.queue.claim_next("other", "other-session"))
            self.clock.advance(0.01)
        last = self.claim()
        self.assertEqual(last.attempt, 4)
        self.assertTrue(self.queue.fail(last, "still unavailable"))
        row = self.queue.get("batch-a")
        self.assertEqual(row["status"], "dead_letter")
        self.assertIsNone(row["next_attempt_at"])
        self.assertEqual((row["attempts"], row["cycle_attempts"]), (4, 4))
        self.clock.advance(10000)
        self.assertIsNone(self.queue.claim_next("other", "other-session"))
        self.assertFalse(self.queue.complete(last))
        self.assertEqual(self.queue.events("batch-a")[-1]["details"]["cause"], "explicit_failure")

    def test_persisted_policy_survives_new_defaults_and_duplicate_enqueue(self):
        self.queue.enqueue("batch-a")
        reopened = BatchQueue(self.path, retry_policy=RetryPolicy(1, 0, 0), clock=self.clock)
        self.assertFalse(reopened.enqueue("batch-a", retry_policy=RetryPolicy(2, 1, 1)))
        claim = reopened.claim_next("worker", "session")
        self.assertTrue(reopened.fail(claim, "failed"))
        row = reopened.get("batch-a")
        self.assertEqual(row["max_attempts"], 4)
        self.assertEqual(row["next_attempt_at"], 1010)
        self.assertEqual(len(reopened.events("batch-a")), 3)

    def test_explicit_delay_retains_legacy_override_but_cannot_bypass_budget(self):
        self.queue.enqueue("batch-a", retry_policy=RetryPolicy(2, 10, 25))
        self.assertTrue(self.queue.fail(self.claim(), "failed", retry_delay_seconds=99))
        self.assertEqual(self.queue.get("batch-a")["next_attempt_at"], 1099)
        self.clock.advance(99)
        self.assertTrue(self.queue.fail(self.claim(), "failed", retry_delay_seconds=0))
        self.assertEqual(self.queue.get("batch-a")["status"], "dead_letter")
        self.assertIsNone(self.queue.claim_next("other", "session"))

    def test_expired_claims_share_budget_with_explicit_failures(self):
        self.queue.enqueue("batch-a", retry_policy=RetryPolicy(2, 10, 20))
        first = self.claim()
        self.clock.advance(30)  # Soft expiration uses strict less-than, not <=.
        self.assertIsNone(self.queue.claim_next("other", "session"))
        self.assertEqual(self.queue.get("batch-a")["status"], "processing")
        self.clock.advance(1)
        self.assertIsNone(self.queue.claim_next("other", "session"))
        self.assertEqual(self.queue.get("batch-a")["next_attempt_at"], 1041)
        for update in (self.queue.refresh_lease, self.queue.complete):
            self.assertFalse(update(first))
        self.assertFalse(self.queue.fail(first, "stale error"))
        self.assertIsNone(self.queue.resume_lease(first, "new-session"))
        self.clock.advance(10)
        second = self.claim()
        self.assertTrue(self.queue.fail(second, "explicit final error"))
        self.assertEqual(self.queue.get("batch-a")["status"], "dead_letter")
        outcomes = [event for event in self.queue.events("batch-a")
                    if event["event_type"] in ("retry_scheduled", "dead_lettered")]
        self.assertEqual([event["details"]["cause"] for event in outcomes],
                         ["lease_expired", "explicit_failure"])

    def test_last_expired_attempt_goes_directly_to_dead_letter(self):
        self.queue.enqueue("batch-a", retry_policy=RetryPolicy(1, 10, 20))
        claim = self.claim()
        self.clock.advance(31)
        self.assertIsNone(self.queue.claim_next("other", "session"))
        self.assertEqual(self.queue.status_counts(), {"dead_letter": 1})
        self.assertEqual(self.queue.get("batch-a")["attempts"], 1)
        self.assertFalse(self.queue.complete(claim))
        event = self.queue.events("batch-a")[-1]
        self.assertEqual((event["event_type"], event["details"]["cause"]),
                         ("dead_lettered", "lease_expired"))

    def test_redrive_retains_history_and_renews_only_cycle_budget(self):
        self.queue.enqueue("batch-a", retry_policy=RetryPolicy(1, 0, 0))
        old = self.claim()
        self.queue.fail(old, "bad configuration")
        history = self.queue.events("batch-a")
        created_at = self.queue.get("batch-a")["created_at"]
        self.assertTrue(self.queue.redrive("batch-a", actor="operator-x", reason="configuration corrected"))
        self.assertFalse(self.queue.redrive("batch-a", actor="operator-y", reason="duplicate click"))
        self.assertEqual(self.queue.events("batch-a")[:len(history)], history)
        row = self.queue.get("batch-a")
        self.assertEqual((row["attempts"], row["cycle_attempts"], row["max_attempts"]), (1, 0, 1))
        self.assertEqual(row["created_at"], created_at)
        self.assertEqual(row["last_error"], "bad configuration")
        replayed = self.claim()
        self.assertEqual(replayed.attempt, 2)
        self.assertFalse(self.queue.complete(old))
        self.assertTrue(self.queue.complete(replayed))
        event = next(e for e in self.queue.events("batch-a") if e["event_type"] == "redriven")
        self.assertEqual((event["actor"], event["reason"], event["attempt"]),
                         ("operator-x", "configuration corrected", 1))

    def test_redrive_rejects_non_dead_letter_states_and_empty_reason(self):
        self.queue.enqueue("batch-a")
        for batch_id in ("missing", "batch-a"):
            self.assertFalse(self.queue.redrive(batch_id, actor="operator", reason="retry"))
        for value in ("", " ", None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.queue.redrive("batch-a", actor="operator", reason=value)
        claim = self.claim()
        self.assertFalse(self.queue.redrive("batch-a", actor="operator", reason="retry"))
        self.queue.fail(claim, "failed")
        self.assertFalse(self.queue.redrive("batch-a", actor="operator", reason="retry"))
        self.clock.advance(10)
        self.queue.complete(self.claim())
        self.assertFalse(self.queue.redrive("batch-a", actor="operator", reason="retry"))

    def test_audit_cursor_and_rejected_mutation_does_not_create_transition(self):
        self.queue.enqueue("batch-a")
        self.queue.enqueue("batch-b")
        claim = self.claim()
        self.queue.refresh_lease(claim)
        resumed = self.queue.resume_lease(claim, "session-new")
        self.queue.complete(resumed)
        original = self.queue.events(limit=1000)
        self.assertFalse(self.queue.complete(claim))
        self.assertEqual(self.queue.events(limit=1000), original)
        seen = []
        cursor = 0
        while page := self.queue.events(after_event_id=cursor, limit=2):
            seen.extend(page)
            cursor = page[-1]["event_id"]
        self.assertEqual(seen, original)
        self.assertEqual([e["event_type"] for e in self.queue.events("batch-a")],
                         ["enqueued", "claimed", "heartbeat", "session_rotated", "completed"])
        self.assertEqual([e["batch_id"] for e in self.queue.events("batch-b")], ["batch-b"])

    def inject_audit_failure(self):
        with sqlite3.connect(self.path) as conn:
            conn.execute("""CREATE TRIGGER abort_audit BEFORE INSERT ON audit_events
                            BEGIN SELECT RAISE(ABORT, 'simulated audit disk failure'); END""")

    def test_audit_failure_rolls_back_failed_claim_and_expired_recovery(self):
        self.queue.enqueue("batch-a", retry_policy=RetryPolicy(1, 0, 0))
        claim = self.claim()
        before_row = self.queue.get("batch-a")
        before_events = self.queue.events()
        self.inject_audit_failure()
        with self.assertRaises(sqlite3.IntegrityError):
            self.queue.fail(claim, "error")
        self.assertEqual(self.queue.get("batch-a"), before_row)
        self.clock.advance(31)
        with self.assertRaises(sqlite3.IntegrityError):
            self.queue.claim_next("other", "session")
        self.assertEqual(self.queue.get("batch-a"), before_row)
        self.assertEqual(self.queue.events(), before_events)

    def test_audit_failure_rolls_back_redrive_and_enqueue(self):
        self.queue.enqueue("batch-a", retry_policy=RetryPolicy(1, 0, 0))
        self.queue.fail(self.claim(), "error")
        before = self.queue.get("batch-a")
        self.inject_audit_failure()
        with self.assertRaises(sqlite3.IntegrityError):
            self.queue.redrive("batch-a", actor="operator", reason="fixed")
        self.assertEqual(self.queue.get("batch-a"), before)
        with self.assertRaises(sqlite3.IntegrityError):
            self.queue.enqueue("batch-new")
        self.assertIsNone(self.queue.get("batch-new"))

    def run_race(self, operation):
        context = multiprocessing.get_context("spawn")
        barrier = context.Barrier(4)
        output = context.Queue()
        processes = [context.Process(target=race_operation,
                     args=(str(self.path), operation, barrier, output)) for _ in range(4)]
        try:
            for process in processes:
                process.start()
            results = [output.get(timeout=30) for _ in processes]
            for process in processes:
                process.join(timeout=30)
                self.assertEqual(process.exitcode, 0)
            return results
        finally:
            for process in processes:
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
            output.close()
            output.join_thread()

    def test_independent_process_redrive_race_has_one_winner_and_one_audit(self):
        self.queue.enqueue("batch-a", retry_policy=RetryPolicy(1, 0, 0))
        self.queue.fail(self.claim(), "error")
        self.assertEqual(sorted(self.run_race("redrive")), [False, False, False, True])
        self.assertEqual(sum(e["event_type"] == "redriven" for e in self.queue.events()), 1)
        self.assertEqual(self.queue.get("batch-a")["attempts"], 1)

    def test_independent_process_expiration_race_cannot_claim_exhausted_batch(self):
        self.queue.enqueue("batch-a", retry_policy=RetryPolicy(1, 0, 0))
        self.claim()
        self.assertEqual(self.run_race("reclaim"), [None] * 4)
        self.assertEqual(sum(e["event_type"] == "dead_lettered" for e in self.queue.events()), 1)
        self.assertEqual(self.queue.get("batch-a")["attempts"], 1)

    def test_policy_and_cursor_validation_and_large_attempt_backoff(self):
        for value in (0, -1, True, 1.5, "3", 2**31):
            with self.subTest(value=value), self.assertRaises(ValueError):
                RetryPolicy(max_attempts=value)
        for value in (-1, float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                RetryPolicy(base_delay_seconds=value)
        with self.assertRaises(ValueError):
            RetryPolicy(base_delay_seconds=20, max_delay_seconds=10)
        self.assertEqual(RetryPolicy(2**31 - 1, 1, 100).delay(2**31 - 1), 100)
        self.assertEqual(RetryPolicy(3, 0, 0).delay(3), 0)
        self.assertEqual(RetryPolicy(3, 0.125, 0.3).delay(3), 0.3)
        for kwargs in ({"limit": 0}, {"limit": 1001}, {"after_event_id": -1}, {"limit": True}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.queue.events(**kwargs)


class MigrationTests(unittest.TestCase):
    def test_original_schema_preserves_rowid_fifo_claims_counts_and_retry_due_time(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "legacy.sqlite3"
            with sqlite3.connect(path) as conn:
                conn.executescript(LEGACY_SCHEMA)
                conn.executemany("""INSERT INTO batches
                    (rowid, batch_id, category, status, attempts, created_at, updated_at,
                     claimed_by, claimed_at, next_attempt_at, claim_id, claim_session)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", [
                    (3, "z-first", "bulk", "ready", 0, 1000, 1000, "", None, 1000, "", ""),
                    (9, "a-second", "urgent", "ready", 0, 1000, 1000, "", None, 1000, "", ""),
                    (10, "active", "normal", "processing", 8, 1000, 1000, "worker", 1000, None, "claim", "session"),
                    (11, "delayed", "normal", "retry", 6, 1000, 1000, "", None, 2000, "", ""),
                    (12, "done", "normal", "done", 2, 1000, 1000, "", None, None, "", ""),
                ])
            queue = BatchQueue(path, clock=lambda: 1000, retry_policy=RetryPolicy(3, 10, 20))
            self.assertEqual(queue.get("active")["max_attempts"], 9)
            self.assertEqual(queue.get("active")["cycle_attempts"], 8)
            self.assertEqual(queue.get("active")["claim_id"], "claim")
            self.assertEqual(queue.get("delayed")["max_attempts"], 7)
            self.assertEqual(queue.get("delayed")["next_attempt_at"], 2000)
            self.assertEqual(queue.get("done")["status"], "done")
            first = queue.claim_next("new", "new", priority_order=())
            second = queue.claim_next("new", "new", priority_order=())
            self.assertEqual([first.batch_id, second.batch_id], ["z-first", "a-second"])
            self.assertTrue(queue.complete(first))
            self.assertTrue(queue.complete(second))
            from reliable_batch_queue import Claim
            old_claim = Claim("active", "worker", "claim", "session", 8, "normal")
            self.assertTrue(queue.complete(old_claim))
            baseline = [e for e in queue.events(limit=1000) if e["event_type"] == "legacy_imported"]
            self.assertEqual(len(baseline), 5)
            reopened = BatchQueue(path, clock=lambda: 1000)
            self.assertEqual(reopened.events(limit=1000), queue.events(limit=1000))
            with sqlite3.connect(path) as conn:
                self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 2)
                self.assertEqual(conn.execute("SELECT rowid FROM batches WHERE batch_id='a-second'").fetchone()[0], 9)
                self.assertEqual(conn.execute("PRAGMA integrity_check").fetchone()[0], "ok")

    def test_migration_failure_rolls_back_schema_rebuild(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "legacy.sqlite3"
            with sqlite3.connect(path) as conn:
                conn.executescript(LEGACY_SCHEMA)
                conn.execute("""INSERT INTO batches (batch_id, status, created_at, updated_at)
                                VALUES ('legacy', 'ready', 1000, 1000)""")
            # A failure after the table rewrite must restore the original CHECK/index/row.
            class BrokenAuditQueue(BatchQueue):
                @staticmethod
                def _event(*args, **kwargs):
                    raise sqlite3.OperationalError("simulated full disk")
            with self.assertRaises(sqlite3.OperationalError):
                BrokenAuditQueue(path)
            with sqlite3.connect(path) as conn:
                columns = {row[1] for row in conn.execute("PRAGMA table_info(batches)")}
                self.assertNotIn("max_attempts", columns)
                self.assertEqual(conn.execute("SELECT batch_id FROM batches").fetchone()[0], "legacy")
                self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 0)
            recovered = BatchQueue(path)
            self.assertEqual(recovered.get("legacy")["status"], "ready")


if __name__ == "__main__":
    unittest.main()
