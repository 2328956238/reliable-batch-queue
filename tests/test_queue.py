from dataclasses import replace
import multiprocessing
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from reliable_batch_queue import BatchQueue, RetryPolicy


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def drain_process(db_path, worker_id, start_barrier, output):
    """Top-level function is picklable under multiprocessing's spawn mode."""
    queue = BatchQueue(db_path)
    start_barrier.wait(timeout=15)
    claims = []
    while (claim := queue.claim_next(worker_id, f"session-{worker_id}")) is not None:
        if not queue.complete(claim):
            raise AssertionError("Unexpected lost claim in no-failure scenario")
        claims.append(claim.batch_id)
    output.put(claims)


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "queue.sqlite3"
        self.clock = Clock()
        self.queue = BatchQueue(self.path, lease_seconds=30, starvation_seconds=100, clock=self.clock,
                                retry_policy=RetryPolicy(base_delay_seconds=0))

    def claim(self, worker="worker-a", session="session-a", **kwargs):
        claim = self.queue.claim_next(worker, session, **kwargs)
        self.assertIsNotNone(claim)
        return claim

    def test_enqueue_is_idempotent_even_after_completion_and_reopen(self):
        self.assertTrue(self.queue.enqueue("batch-a", "normal"))
        self.assertFalse(self.queue.enqueue("batch-a", "urgent"))
        claim = self.claim()
        self.assertEqual(claim.category, "normal")
        self.assertTrue(self.queue.complete(claim))
        reopened = BatchQueue(self.path, clock=self.clock)
        self.assertFalse(reopened.enqueue("batch-a"))
        self.assertEqual(reopened.get("batch-a")["status"], "done")
        self.assertFalse(reopened.complete(claim))
        self.assertEqual(reopened.status_counts(), {"done": 1})

    def test_priority_then_fifo_and_deterministic_equal_timestamp_tie(self):
        self.queue.enqueue("bulk-first", "bulk")
        self.queue.enqueue("urgent-first", "urgent")
        self.queue.enqueue("urgent-second", "urgent")
        self.queue.enqueue("normal-first", "normal")
        self.queue.enqueue("unknown-first", "custom")
        expected = ["urgent-first", "urgent-second", "normal-first", "bulk-first", "unknown-first"]
        actual = [self.claim().batch_id for _ in expected]
        self.assertEqual(actual, expected)
        self.assertIsNone(self.queue.claim_next("worker-b", "session-b"))

    def test_empty_priority_order_is_strict_fifo(self):
        self.queue.enqueue("older", "bulk")
        self.clock.advance(1)
        self.queue.enqueue("newer", "urgent")
        self.assertEqual(self.claim(priority_order=()).batch_id, "older")

    def test_aged_batches_precede_fresh_priority_batches_in_fifo_order(self):
        self.queue.enqueue("oldest", "bulk")
        self.clock.advance(10)
        self.queue.enqueue("old", "normal")
        self.clock.advance(100)
        self.queue.enqueue("fresh", "urgent")
        self.assertEqual(self.claim().batch_id, "oldest")
        self.assertEqual(self.claim().batch_id, "old")
        self.assertEqual(self.claim().batch_id, "fresh")

    def test_retry_delay_attempt_count_and_error_truncation(self):
        self.queue.enqueue("batch-a")
        first = self.claim()
        self.assertEqual(first.attempt, 1)
        self.assertTrue(self.queue.fail(first, "x" * 2000, retry_delay_seconds=150))
        self.clock.advance(120)  # Aging does not bypass retry eligibility.
        self.assertIsNone(self.queue.claim_next("worker-b", "session-b"))
        self.assertEqual(len(self.queue.get("batch-a")["last_error"]), 1000)
        self.clock.advance(30)
        retried = self.claim("worker-b", "session-b")
        self.assertEqual(retried.attempt, 2)
        self.assertNotEqual(first.claim_id, retried.claim_id)
        self.assertTrue(self.queue.complete(retried))
        self.assertEqual(self.queue.get("batch-a")["last_error"], "")

    def test_aged_equal_timestamp_batches_use_insertion_order(self):
        self.queue.enqueue("first", "bulk")
        self.queue.enqueue("second", "urgent")
        self.clock.advance(100)
        self.assertEqual(self.claim().batch_id, "first")
        self.assertEqual(self.claim().batch_id, "second")

    def test_heartbeat_prevents_reclaim_until_last_heartbeat_is_stale(self):
        self.queue.enqueue("batch-a")
        first = self.claim()
        self.clock.advance(20)
        self.assertTrue(self.queue.refresh_lease(first))
        self.clock.advance(20)
        self.assertIsNone(self.queue.claim_next("worker-b", "session-b"))
        self.clock.advance(11)
        second = self.claim("worker-b", "session-b")
        self.assertEqual(second.attempt, 2)

    def test_reclaimed_lease_rejects_every_old_mutation(self):
        self.queue.enqueue("batch-a")
        first = self.claim()
        self.clock.advance(31)
        second = self.claim("worker-b", "session-b")
        self.assertFalse(self.queue.refresh_lease(first))
        self.assertFalse(self.queue.complete(first))
        self.assertFalse(self.queue.fail(first, "old error"))
        self.assertIsNone(self.queue.resume_lease(first, "session-restarted"))
        self.assertEqual(self.queue.get("batch-a")["claim_id"], second.claim_id)
        self.assertTrue(self.queue.complete(second))

    def test_soft_expiration_allows_heartbeat_before_reclaim(self):
        self.queue.enqueue("batch-a")
        claim = self.claim()
        self.clock.advance(31)
        self.assertTrue(self.queue.refresh_lease(claim))
        self.assertIsNone(self.queue.claim_next("worker-b", "session-b"))

    def test_all_three_ownership_fields_are_required_for_every_mutation(self):
        self.queue.enqueue("batch-a")
        claim = self.claim()
        for field in ("worker_id", "claim_id", "session_id"):
            with self.subTest(field=field):
                wrong = replace(claim, **{field: "wrong-owner"})
                self.assertFalse(self.queue.refresh_lease(wrong))
                self.assertFalse(self.queue.complete(wrong))
                self.assertFalse(self.queue.fail(wrong, "error"))
                self.assertIsNone(self.queue.resume_lease(wrong, "next-session"))
        self.assertTrue(self.queue.complete(claim))

    def test_session_rotation_is_compare_and_swap(self):
        self.queue.enqueue("batch-a")
        first = self.claim()
        second = self.queue.resume_lease(first, "session-restart")
        self.assertIsNotNone(second)
        self.assertEqual(first.claim_id, second.claim_id)
        self.assertEqual(first.attempt, second.attempt)
        self.assertIsNone(self.queue.resume_lease(first, "racing-restart"))
        self.assertFalse(self.queue.refresh_lease(first))
        self.assertFalse(self.queue.complete(first))
        self.assertFalse(self.queue.fail(first, "error"))
        self.assertTrue(self.queue.complete(second))

    def test_invalid_identity_and_policy_inputs_are_rejected(self):
        for value in ("", " ", None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.queue.claim_next(value, "session-a")
        for value in (0, -1, float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                BatchQueue(self.path, lease_seconds=value)
        self.queue.enqueue("batch-a")
        claim = self.claim()
        with self.assertRaises(ValueError):
            self.queue.fail(claim, "error", retry_delay_seconds=-1)
        with self.assertRaises(ValueError):
            self.queue.complete(replace(claim, session_id=""))
        with self.assertRaises(TypeError):
            self.queue.complete("batch-a")
        with self.assertRaises(ValueError):
            self.queue.resume_lease(claim, claim.session_id)
        self.assertEqual(self.queue.get("batch-a")["status"], "processing")

    def test_sql_parameters_accept_quotes_without_interpreting_sql(self):
        batch_id = "batch'; DROP TABLE batches; --"
        category = "custom'category"
        self.queue.enqueue(batch_id, category)
        claim = self.claim(priority_order=(category,))
        self.assertEqual(claim.batch_id, batch_id)
        self.assertTrue(self.queue.complete(claim))
        self.assertEqual(self.queue.status_counts(), {"done": 1})

    def test_no_duplicate_claims_across_independent_processes(self):
        # This exercises SQLite locking, not just a process-local Python lock.
        total = 80
        for number in range(total):
            self.queue.enqueue(f"batch-{number:03d}")
        context = multiprocessing.get_context("spawn")
        barrier = context.Barrier(4)
        output = context.Queue()
        processes = [context.Process(target=drain_process, args=(str(self.path), f"worker-{i}", barrier, output))
                     for i in range(4)]
        try:
            for process in processes:
                process.start()
            claimed = [item for _ in processes for item in output.get(timeout=30)]
            for process in processes:
                process.join(timeout=30)
                self.assertEqual(process.exitcode, 0)
            self.assertEqual(len(claimed), total)
            self.assertEqual(len(set(claimed)), total)
            self.assertEqual(self.queue.status_counts(), {"done": total})
            self.assertTrue(all(self.queue.get(batch_id)["attempts"] == 1 for batch_id in claimed))
        finally:
            for process in processes:
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
            output.close()
            output.join_thread()


if __name__ == "__main__":
    unittest.main()
