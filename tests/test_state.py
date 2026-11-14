from gaensestall.core.models import Job, JobResult, JobState, utc_now
from gaensestall.core.settings import Settings
from gaensestall.kafka.state import StatePublisher


class FlakyProducer:
    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.sent: list[dict] = []
        self.attempts = 0

    def send(self, **kwargs) -> None:
        self.attempts += 1
        if self.failures:
            self.failures -= 1
            raise RuntimeError("broker unavailable")
        self.sent.append(kwargs)


def _publisher(failures: int) -> tuple[StatePublisher, FlakyProducer]:
    producer = FlakyProducer(failures)
    return StatePublisher(Settings.from_env(), producer), producer  # type: ignore[arg-type]


def _result(job: Job) -> JobResult:
    now = utc_now()
    return JobResult(
        job_id=job.job_id,
        run_id=job.run_id,
        status="succeeded",
        result="done",
        worker_id="worker-1",
        started_at=now,
        finished_at=now,
    )


def test_state_publish_reports_a_failure() -> None:
    publisher, producer = _publisher(failures=1)
    job = Job(instruction="Hello")

    assert publisher.queued(job) is False
    assert producer.attempts == 1
    assert producer.sent == []


def test_state_publish_reports_success() -> None:
    publisher, producer = _publisher(failures=0)
    job = Job(instruction="Hello")

    assert publisher.queued(job) is True
    assert len(producer.sent) == 1


def test_completed_state_reflects_the_job_result() -> None:
    publisher, producer = _publisher(failures=0)
    job = Job(instruction="Hello")
    state = JobState.queued(job)

    assert publisher.completed(state, _result(job)) is True

    sent = producer.sent[0]
    assert sent["key"] == job.job_id
    assert sent["model"].status == "succeeded"
    assert sent["model"].result == "done"


def test_running_state_is_returned_for_later_updates() -> None:
    publisher, _ = _publisher(failures=0)
    job = Job(instruction="Hello")

    state, published = publisher.running(job, worker_id="worker-1")

    assert published is True
    assert isinstance(state, JobState)
    assert state.status == "running"
    assert state.started_at is not None
    assert state.worker_id == "worker-1"
