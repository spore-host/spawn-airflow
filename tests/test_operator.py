"""Unit-test the operator + trigger with a fake spawn CLI (no AWS, no scheduler)."""

import asyncio

import pytest
from airflow.exceptions import AirflowException

from spawn_airflow import SpawnRunTaskOperator, SpawnTaskStatusTrigger


class FakeCompleted:
    def __init__(self, rc=0, out=""):
        self.returncode = rc
        self.stdout = out
        self.stderr = ""


def _op(**kw):
    defaults = dict(
        task_id="align",
        command="echo hi",
        workdir_s3="s3://b/runs/align",
        region="us-east-1",
        poll_interval=0,
    )
    defaults.update(kw)
    return SpawnRunTaskOperator(**defaults)


def test_execute_sync_dispatches_detached_and_returns_on_exit0(monkeypatch):
    calls = []

    def fake_run(self, argv, check):
        calls.append(list(argv))
        if argv[:3] == ["spawn", "task", "status"] and "--check-complete" in argv:
            return FakeCompleted(0)  # completed
        if argv[:3] == ["spawn", "task", "status"] and "json" in argv:
            return FakeCompleted(0, '{"task_id":"t","exit_code":0,"state":"completed"}')
        return FakeCompleted(0)

    monkeypatch.setattr(SpawnRunTaskOperator, "_run_argv", fake_run)
    op = _op()
    result = op.execute({"ti": None})

    run_argv = next(c for c in calls if c[:3] == ["spawn", "task", "run"])
    assert "--spec" in run_argv
    assert "--wait" not in run_argv  # detached
    assert result == "s3://b/runs/align"


def test_execute_sync_raises_on_nonzero(monkeypatch):
    def fake_run(self, argv, check):
        if argv[:3] == ["spawn", "task", "status"] and "--check-complete" in argv:
            return FakeCompleted(1)  # failed
        if argv[:3] == ["spawn", "task", "status"] and "json" in argv:
            return FakeCompleted(1, '{"exit_code":137,"state":"failed"}')
        return FakeCompleted(0)

    monkeypatch.setattr(SpawnRunTaskOperator, "_run_argv", fake_run)
    with pytest.raises(AirflowException) as ei:
        _op().execute({"ti": None})
    assert "137" in str(ei.value)


def test_execute_deferrable_defers(monkeypatch):
    try:
        from airflow.sdk.exceptions import TaskDeferred  # Airflow 3.x
    except ImportError:
        from airflow.exceptions import TaskDeferred  # Airflow 2.x

    monkeypatch.setattr(SpawnRunTaskOperator, "_run_argv", lambda self, argv, check: FakeCompleted(0))
    op = _op(deferrable=True)
    with pytest.raises(TaskDeferred) as ei:
        op.execute({"ti": None})
    assert isinstance(ei.value.trigger, SpawnTaskStatusTrigger)
    assert ei.value.method_name == "execute_complete"


def test_execute_complete_success_and_failure():
    op = _op()
    assert op.execute_complete({}, {"exit_code": 0}) == "s3://b/runs/align"
    with pytest.raises(AirflowException):
        op.execute_complete({}, {"exit_code": 2})


def test_on_kill_terminates_by_task_id(monkeypatch):
    calls = []
    monkeypatch.setattr(
        SpawnRunTaskOperator, "_run_argv", lambda self, argv, check: calls.append(list(argv))
    )
    op = _op()
    op._task_id = "af-align-1"
    op.on_kill()
    assert calls and calls[0] == ["spawn", "terminate", "af-align-1", "--region", "us-east-1", "--yes"]


def test_trigger_serialize_roundtrip_and_run(monkeypatch):
    trig = SpawnTaskStatusTrigger("af-align-1", "us-east-1", poll_interval=0)
    classpath, kwargs = trig.serialize()
    assert classpath.endswith("SpawnTaskStatusTrigger")
    assert kwargs == {"task_id": "af-align-1", "region": "us-east-1", "poll_interval": 0}

    # run(): first check-complete says running (rc=2), then completed (rc=0) → one event
    rcs = [2, 0]
    monkeypatch.setattr(trig, "_probe_check_complete", lambda: rcs.pop(0))
    monkeypatch.setattr(trig, "_fetch_exit_code", lambda: 0)

    async def drain():
        return [ev async for ev in trig.run()]

    events = asyncio.run(drain())
    assert len(events) == 1
    assert events[0].payload["exit_code"] == 0
    assert events[0].payload["task_id"] == "af-align-1"
