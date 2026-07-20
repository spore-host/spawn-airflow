"""SpawnRunTaskOperator: run an Airflow task's command on an ephemeral EC2
instance via spore-host/spawn.

Mirrors ``EcsRunTaskOperator``: ``execute`` submits the task through
``spawn task run`` (detached) then either polls the durable completion record via
``spawn task status`` (sync) or ``defer``s to ``SpawnTaskStatusTrigger``
(deferrable, frees the worker slot); ``execute_complete`` resumes and fails the
task on a nonzero exit. spawn owns sizing (truffle), S3 staging, the container
run, the scoped IAM profile, and the durable completion record (spawn#386). The
instance self-terminates on completion (``on_complete=terminate`` + TTL);
``on_kill`` best-effort ``spawn terminate``s if the task is killed before then.

We subclass ``BaseOperator`` directly (no AWS hook — spawn is a CLI, like
``KubernetesPodOperator`` wraps kubectl).
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import time
from datetime import timedelta
from typing import Any, Optional, Sequence

from airflow.configuration import conf
from airflow.exceptions import AirflowException

try:  # Airflow 3.x canonical location
    from airflow.sdk.bases.operator import BaseOperator
except ImportError:  # Airflow 2.x fallback
    from airflow.models.baseoperator import BaseOperator  # type: ignore[no-redef]

from . import taskspec
from .trigger import SpawnTaskStatusTrigger

_NAME_SANITIZE = re.compile(r"[^a-z0-9-]+")

# Where the task runs on the instance. Must be user-writable: spawn runs the
# command as the instance's unprivileged login user (`su - <user>`), which cannot
# create dirs under the root-owned `/mnt`. `/var/tmp` is world-writable (1777) and
# disk-backed (not tmpfs, unlike `/tmp`).
JOB_DIR = "/var/tmp/spawn_airflow_job"


class SpawnRunTaskOperator(BaseOperator):
    """Run ``command`` on an ephemeral EC2 instance sized/launched via spawn."""

    template_fields: Sequence[str] = ("command", "workdir_s3")
    ui_color = "#5b8a72"

    def __init__(
        self,
        *,
        command: str,
        workdir_s3: str,
        region: str = "us-east-1",
        ttl: str = "4h",
        instance_type: Optional[str] = None,
        cpus: Optional[int] = None,
        memory_gib: Optional[float] = None,
        spot: bool = False,
        poll_interval: float = 15.0,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.command = command
        self.workdir_s3 = workdir_s3
        self.region = region
        self.ttl = ttl
        self.instance_type = instance_type
        self.cpus = cpus
        self.memory_gib = memory_gib
        self.spot = spot
        self.poll_interval = poll_interval
        self.deferrable = deferrable
        self._task_id: Optional[str] = None

    # ---- helpers ---------------------------------------------------------

    def _spawn_task_id(self, context: Any) -> str:
        ti = (context or {}).get("ti") if isinstance(context, dict) else None
        raw = f"af-{self.task_id}-{getattr(ti, 'try_number', 1) if ti else 1}"
        return _NAME_SANITIZE.sub("-", raw.lower()).strip("-")[:60] or "af-task"

    def _run_argv(self, argv: list[str], check: bool) -> "subprocess.CompletedProcess[str]":
        return subprocess.run(argv, check=check, capture_output=True, text=True)

    # ---- the seam --------------------------------------------------------

    def execute(self, context: Any) -> Any:
        task_id = self._spawn_task_id(context)
        self._task_id = task_id

        spec = taskspec.build_task_spec(
            task_id=task_id,
            command=self.command,
            job_dir=JOB_DIR,
            workdir_s3=self.workdir_s3,
            cpus=self.cpus,
            memory_gib=self.memory_gib,
            instance_hint=self.instance_type,
            spot=self.spot,
            ttl=self.ttl,
            on_complete="terminate",
        )
        with tempfile.NamedTemporaryFile(
            "w", suffix=".json", prefix=f"spawn-airflow-{task_id}-", delete=False
        ) as fh:
            json.dump(spec, fh)
            spec_file = fh.name

        try:
            self.log.info("spawn-airflow: dispatching %s via `spawn task run`", task_id)
            # Launch DETACHED (no --wait): spawn sizes, launches, and the instance
            # writes its own completion record. We poll/defer below.
            self._run_argv(
                ["spawn", "task", "run", "--spec", spec_file, "--region", self.region],
                check=True,
            )
        finally:
            try:
                os.unlink(spec_file)
            except OSError:
                pass

        if self.deferrable:
            self.defer(
                trigger=SpawnTaskStatusTrigger(task_id, self.region, self.poll_interval),
                method_name="execute_complete",
                timeout=timedelta(hours=24),
            )

        # Synchronous poll (non-deferrable path).
        probe = ["spawn", "task", "status", task_id, "--region", self.region, "--check-complete"]
        while True:
            out = self._run_argv(probe, check=False)
            status = taskspec.check_complete_to_status(out.returncode)
            if status is not None:
                return self._finish(self._fetch_exit_code(task_id))
            time.sleep(self.poll_interval)

    def _fetch_exit_code(self, task_id: str) -> int:
        """Read the exit code from the CompletionRecord (`spawn task status <id>
        -o json`). Defaults to 1 if the record can't be read/parsed."""
        out = self._run_argv(
            ["spawn", "task", "status", task_id, "--region", self.region, "-o", "json"],
            check=False,
        )
        try:
            rec = taskspec.parse_completion_record(out.stdout)
            return int(rec.get("exit_code", 1))
        except Exception:
            self.log.warning("spawn-airflow: could not parse completion record for %s", task_id)
            return 1

    def execute_complete(self, context: Any, event: Any) -> Any:
        payload = event or {}
        code = payload.get("exit_code") if isinstance(payload, dict) else None
        return self._finish(int(code) if code is not None else 1)

    def _finish(self, code: int) -> str:
        if code != 0:
            raise AirflowException(
                f"spawn-airflow: task command exited with code {code} "
                f"(see {self.workdir_s3}/stdout.txt, /stderr.txt)"
            )
        self.log.info("spawn-airflow: task succeeded; outputs under %s", self.workdir_s3)
        return self.workdir_s3

    def on_kill(self) -> None:
        # Best-effort teardown if the task is killed before the instance self-terminates.
        if self._task_id:
            self._run_argv(
                ["spawn", "terminate", self._task_id, "--region", self.region, "--yes"],
                check=False,
            )
