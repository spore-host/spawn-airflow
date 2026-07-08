"""SpawnRunTaskOperator: run an Airflow task's command on an ephemeral EC2
instance via spore-host/spawn.

Mirrors ``EcsRunTaskOperator``: ``execute`` submits (launches the instance,
non-blocking) then either polls the durable ``.exitcode`` in S3 (sync) or
``defer``s to ``SpawnExitCodeTrigger`` (deferrable, frees the worker slot);
``execute_complete`` resumes and fails the task on a nonzero exit. The instance
self-terminates on completion (``--on-complete terminate`` + TTL); ``on_kill``
best-effort ``spawn terminate``s if the task is killed before then.

We subclass ``BaseOperator`` directly (no AWS hook — spawn is a CLI, like
``KubernetesPodOperator`` wraps kubectl).
"""

from __future__ import annotations

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

from . import bootstrap, completion, launch, sizing
from .trigger import SpawnExitCodeTrigger

_NAME_SANITIZE = re.compile(r"[^a-z0-9-]+")


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
        self._instance_name: Optional[str] = None

    # ---- helpers ---------------------------------------------------------

    def _name(self, context: Any) -> str:
        ti = (context or {}).get("ti") if isinstance(context, dict) else None
        raw = f"af-{self.task_id}-{getattr(ti, 'try_number', 1) if ti else 1}"
        return _NAME_SANITIZE.sub("-", raw.lower()).strip("-")[:60] or "af-task"

    def _instance_type(self) -> str:
        mem_mb = int(self.memory_gib * 1024) if self.memory_gib else None
        return sizing.pick_instance_type(
            override=self.instance_type, cores=self.cpus, mem_mb=mem_mb
        )

    def _run_argv(self, argv: list[str], check: bool) -> "subprocess.CompletedProcess[str]":
        return subprocess.run(argv, check=check, capture_output=True, text=True)

    # ---- the seam --------------------------------------------------------

    def execute(self, context: Any) -> Any:
        name = self._name(context)
        self._instance_name = name
        user_data = bootstrap.build_user_data(
            workdir_s3=self.workdir_s3, region=self.region, command=self.command
        )
        with tempfile.NamedTemporaryFile(
            "w", suffix=".sh", prefix=f"spawn-airflow-{name}-", delete=False
        ) as fh:
            fh.write(user_data)
            user_data_file = fh.name

        spec = launch.LaunchSpec(
            name=name,
            instance_type=self._instance_type(),
            region=self.region,
            user_data_file=user_data_file,
            ttl=self.ttl,
            spot=self.spot,
        )
        try:
            self.log.info("spawn-airflow: launching %s (%s)", name, spec.instance_type)
            self._run_argv(launch.build_launch_argv(spec), check=True)
        finally:
            try:
                os.unlink(user_data_file)
            except OSError:
                pass

        if self.deferrable:
            self.defer(
                trigger=SpawnExitCodeTrigger(self.workdir_s3, self.region, self.poll_interval),
                method_name="execute_complete",
                timeout=timedelta(hours=24),
            )

        # Synchronous poll (non-deferrable path).
        probe = completion.build_exitcode_probe_argv(self.workdir_s3, self.region)
        while True:
            out = self._run_argv(probe, check=False)
            if out.returncode == 0:
                code = completion.parse_exit_code(out.stdout)
                if code is not None:
                    return self._finish(code)
            time.sleep(self.poll_interval)

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
        if self._instance_name:
            self._run_argv(launch.build_cancel_argv(self._instance_name), check=False)
