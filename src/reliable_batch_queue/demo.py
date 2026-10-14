"""Offline, deterministic two-worker scenario; all data is temporary."""

from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

from .queue import BatchQueue, RetryPolicy


@dataclass
class DemoClock:
    now: float = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def main() -> None:
    clock = DemoClock()
    with TemporaryDirectory(prefix="batch-queue-demo-") as directory:
        path = Path(directory) / "queue.sqlite3"
        # Separate queue objects represent workers accessing one coordinator DB.
        policy = RetryPolicy(max_attempts=3, base_delay_seconds=10, max_delay_seconds=15)
        worker_a = BatchQueue(path, lease_seconds=30, retry_policy=policy, clock=clock)
        worker_b = BatchQueue(path, lease_seconds=30, retry_policy=policy, clock=clock)
        worker_a.enqueue("batch-alpha", "urgent")
        worker_a.enqueue("batch-beta", "normal")

        old_claim = worker_a.claim_next("worker-a", "session-a1")
        second_claim = worker_b.claim_next("worker-b", "session-b1")
        assert old_claim is not None and second_claim is not None
        assert old_claim.batch_id == "batch-alpha"
        assert second_claim.batch_id == "batch-beta"
        print(f"1. worker-a claimed {old_claim.batch_id}; worker-b claimed {second_claim.batch_id}")
        assert worker_b.complete(second_claim)
        print("2. worker-b completed batch-beta; worker-a stopped heartbeating")

        clock.advance(31)
        assert worker_b.claim_next("worker-b", "session-b1") is None
        assert worker_b.get("batch-alpha")["status"] == "retry"
        print("3. +31s: expired attempt 1 scheduled with 10s backoff")
        clock.advance(10)
        recovered = worker_b.claim_next("worker-b", "session-b1")
        assert recovered is not None and recovered.batch_id == old_claim.batch_id
        assert recovered.attempt == 2
        assert not worker_a.refresh_lease(old_claim)
        assert not worker_a.complete(old_claim)
        assert not worker_a.fail(old_claim, "stale process report")
        print("4. +10s: worker-b claimed attempt 2; stale worker-a writes rejected")

        assert worker_b.fail(recovered, "simulated transient failure")
        assert worker_b.get("batch-alpha")["next_attempt_at"] == clock.now + 15
        assert worker_a.claim_next("worker-a", "session-a2") is None
        print("5. attempt 2 failed: exponential backoff capped at 15s")
        clock.advance(15)
        retried = worker_a.claim_next("worker-a", "session-a2")
        assert retried is not None and retried.attempt == 3
        clock.advance(31)
        assert worker_b.claim_next("worker-b", "session-b1") is None
        assert worker_b.get("batch-alpha")["status"] == "dead_letter"
        print("6. attempt 3 expired: budget exhausted; moved to dead_letter")

        assert worker_a.redrive("batch-alpha", actor="demo-operator", reason="Downstream configuration repaired")
        replayed = worker_a.claim_next("worker-a", "session-a3")
        assert replayed is not None and replayed.attempt == 4
        assert worker_a.get("batch-alpha")["cycle_attempts"] == 1
        resumed = worker_a.resume_lease(replayed, "session-a4")
        assert resumed is not None
        assert not worker_a.complete(replayed)
        assert worker_a.complete(resumed)
        print("7. reasoned redrive completed attempt 4 (new cycle=1); old session fenced")
        outcomes = [event["event_type"] for event in worker_a.events("batch-alpha")
                    if event["event_type"] in ("retry_scheduled", "dead_lettered", "redriven", "completed")]
        assert outcomes == ["retry_scheduled", "retry_scheduled", "dead_lettered", "redriven", "completed"]
        print(f"8. audit trail: {' -> '.join(outcomes)}")
        assert worker_a.status_counts() == {"done": 2}
        print(f"9. final status: {worker_a.status_counts()}")
    print("Temporary database removed. No network access or external services used.")


if __name__ == "__main__":
    main()
