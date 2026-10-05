"""Offline, deterministic two-worker scenario; all data is temporary."""

from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

from .queue import BatchQueue


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
        worker_a = BatchQueue(path, lease_seconds=30, clock=clock)
        worker_b = BatchQueue(path, lease_seconds=30, clock=clock)
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
        recovered = worker_b.claim_next("worker-b", "session-b1")
        assert recovered is not None and recovered.batch_id == old_claim.batch_id
        assert recovered.attempt == 2
        print("3. simulated +31 seconds: worker-b reclaimed batch-alpha (attempt=2)")
        assert not worker_a.refresh_lease(old_claim)
        assert not worker_a.complete(old_claim)
        assert not worker_a.fail(old_claim, "stale process report")
        print("4. stale worker-a heartbeat / complete / fail: all rejected")

        assert worker_b.fail(recovered, "simulated transient failure", retry_delay_seconds=10)
        assert worker_a.claim_next("worker-a", "session-a2") is None
        clock.advance(10)
        retried = worker_a.claim_next("worker-a", "session-a2")
        assert retried is not None and retried.attempt == 3
        resumed = worker_a.resume_lease(retried, "session-a3")
        assert resumed is not None
        assert not worker_a.complete(retried)
        assert worker_a.complete(resumed)
        print("5. +10 seconds: retry succeeded; restarted session fenced its older session")
        assert worker_a.status_counts() == {"done": 2}
        print(f"6. final status: {worker_a.status_counts()}")
    print("Temporary database removed. No network access or external services used.")


if __name__ == "__main__":
    main()
