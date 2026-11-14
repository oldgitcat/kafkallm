from dataclasses import replace

import pytest

from kafkallm.core.settings import Settings
from kafkallm.kafka.kafka import wait_for_kafka


def test_wait_for_kafka_honors_disabled_startup_timeout() -> None:
    settings = replace(Settings.from_env(), kafka_startup_timeout_seconds=0)

    with pytest.raises(RuntimeError, match="startup timeout"):
        wait_for_kafka(settings)
