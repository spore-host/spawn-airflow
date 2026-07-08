"""SpawnExitCodeTrigger: async-poll the durable ``.exitcode`` object in S3.

Runs in Airflow's triggerer (asyncio) so a deferred task frees its worker slot
while the ephemeral instance runs. Reuses the pure ``completion.py`` probe via
``asyncio.to_thread`` — no aioboto3 dependency; the sync ``aws s3 cp`` subprocess
runs in a thread. Yields a single ``TriggerEvent`` once ``.exitcode`` appears.
"""

from __future__ import annotations

import asyncio
import subprocess
from typing import Any, AsyncIterator

from airflow.triggers.base import BaseTrigger, TriggerEvent

from . import completion


class SpawnExitCodeTrigger(BaseTrigger):
    """Fire once the ``.exitcode`` object exists under the task's S3 prefix."""

    def __init__(self, s3_prefix: str, region: str, poll_interval: float = 15.0) -> None:
        super().__init__()
        self.s3_prefix = s3_prefix
        self.region = region
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "spawn_airflow.trigger.SpawnExitCodeTrigger",
            {
                "s3_prefix": self.s3_prefix,
                "region": self.region,
                "poll_interval": self.poll_interval,
            },
        )

    def _probe(self) -> "subprocess.CompletedProcess[str]":
        argv = completion.build_exitcode_probe_argv(self.s3_prefix, self.region)
        return subprocess.run(argv, capture_output=True, text=True)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        while True:
            out = await asyncio.to_thread(self._probe)
            if out.returncode == 0:
                code = completion.parse_exit_code(out.stdout)
                if code is not None:
                    yield TriggerEvent({"exit_code": code, "s3_prefix": self.s3_prefix})
                    return
            await asyncio.sleep(self.poll_interval)
