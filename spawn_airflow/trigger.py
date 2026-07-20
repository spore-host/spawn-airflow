"""SpawnTaskStatusTrigger: async-poll the durable completion record via
``spawn task status``.

Runs in Airflow's triggerer (asyncio) so a deferred task frees its worker slot
while the ephemeral instance runs. Polls ``spawn task status <id> --check-complete``
via ``asyncio.to_thread`` (the sync subprocess runs in a thread — no async AWS
dep), then reads the exit code from the CompletionRecord. Yields a single
``TriggerEvent`` once the task completes.
"""

from __future__ import annotations

import asyncio
import subprocess
from typing import Any, AsyncIterator

from airflow.triggers.base import BaseTrigger, TriggerEvent

from . import taskspec


class SpawnTaskStatusTrigger(BaseTrigger):
    """Fire once the task's completion record exists (via ``spawn task status``)."""

    def __init__(self, task_id: str, region: str, poll_interval: float = 15.0) -> None:
        super().__init__()
        self.task_id = task_id
        self.region = region
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "spawn_airflow.trigger.SpawnTaskStatusTrigger",
            {
                "task_id": self.task_id,
                "region": self.region,
                "poll_interval": self.poll_interval,
            },
        )

    def _probe_check_complete(self) -> int:
        argv = [
            "spawn", "task", "status", self.task_id, "--region", self.region, "--check-complete",
        ]
        return subprocess.run(argv, capture_output=True, text=True).returncode

    def _fetch_exit_code(self) -> int:
        argv = ["spawn", "task", "status", self.task_id, "--region", self.region, "-o", "json"]
        out = subprocess.run(argv, capture_output=True, text=True)
        try:
            return int(taskspec.parse_completion_record(out.stdout).get("exit_code", 1))
        except Exception:
            return 1

    async def run(self) -> AsyncIterator[TriggerEvent]:
        while True:
            rc = await asyncio.to_thread(self._probe_check_complete)
            status = taskspec.check_complete_to_status(rc)
            if status is not None:
                code = await asyncio.to_thread(self._fetch_exit_code)
                yield TriggerEvent({"exit_code": code, "task_id": self.task_id})
                return
            await asyncio.sleep(self.poll_interval)
