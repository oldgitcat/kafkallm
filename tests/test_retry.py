import logging
import time

import pytest

from kafkallm.worker.retry import RetryTimeout, retry_until


def test_retry_returns_the_first_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("kafkallm.worker.retry.time.sleep", lambda _seconds: None)
    calls: list[int] = []

    def attempt() -> str:
        calls.append(len(calls))
        if len(calls) < 3:
            raise RuntimeError("not yet")
        return "ok"

    result = retry_until(
        time.monotonic() + 5,
        attempt,
        description="test",
        logger=logging.getLogger("test"),
    )

    assert result == "ok"
    assert len(calls) == 3


def test_retry_reports_the_deadline() -> None:
    def attempt() -> None:
        raise RuntimeError("broker down")

    with pytest.raises(RetryTimeout, match="test did not become ready"):
        retry_until(
            time.monotonic() - 1,
            attempt,
            description="test",
            logger=logging.getLogger("test"),
        )


def test_retry_keeps_the_last_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("kafkallm.worker.retry.time.sleep", lambda _seconds: None)

    def attempt() -> None:
        raise RuntimeError("broker down")

    with pytest.raises(RetryTimeout) as caught:
        retry_until(
            time.monotonic() + 0.02,
            attempt,
            description="test",
            logger=logging.getLogger("test"),
        )

    assert isinstance(caught.value.last_error, RuntimeError)
