from dataclasses import replace

import pytest

from kafkallm.kafka import wait_for_kafka
from kafkallm.settings import Settings


def test_wait_for_kafka_honors_disabled_startup_timeout() -> None:
    settings = replace(Settings.from_env(), kafka_startup_timeout_seconds=0)

    with pytest.raises(RuntimeError, match="Startup-Timeouts"):
        wait_for_kafka(settings)
