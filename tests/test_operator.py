"""Unit-test the operator + trigger with a fake spawn/aws (no AWS, no scheduler)."""

import asyncio

import pytest
from airflow.exceptions import AirflowException

from spawn_airflow import SpawnExitCodeTrigger, SpawnRunTaskOperator


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


def test_execute_sync_launches_and_returns_on_exit0(monkeypatch):
    calls = []

    def fake_run(self, argv, check):
        calls.append(list(argv))
        if argv[:2] == ["spawn", "launch"]:
            return FakeCompleted(0)
        if argv[:3] == ["aws", "s3", "cp"] and argv[3].endswith(".exitcode"):
            return FakeCompleted(0, "0\n")
        return FakeCompleted(0)

    monkeypatch.setattr(SpawnRunTaskOperator, "_run_argv", fake_run)
    op = _op()
    result = op.execute({"ti": None})

    assert any(c[:2] == ["spawn", "launch"] for c in calls)
    launch_argv = next(c for c in calls if c[:2] == ["spawn", "launch"])
    assert launch_argv[launch_argv.index("--on-complete") + 1] == "terminate"
    assert result == "s3://b/runs/align"


def test_execute_sync_raises_on_nonzero(monkeypatch):
    def fake_run(self, argv, check):
        if argv[:2] == ["spawn", "launch"]:
            return FakeCompleted(0)
        if argv[3].endswith(".exitcode"):
            return FakeCompleted(0, "137\n")
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

    def fake_run(self, argv, check):
        return FakeCompleted(0)

    monkeypatch.setattr(SpawnRunTaskOperator, "_run_argv", fake_run)
    op = _op(deferrable=True)
    with pytest.raises(TaskDeferred) as ei:
        op.execute({"ti": None})
    assert isinstance(ei.value.trigger, SpawnExitCodeTrigger)
    assert ei.value.method_name == "execute_complete"


def test_execute_complete_success_and_failure():
    op = _op()
    assert op.execute_complete({}, {"exit_code": 0}) == "s3://b/runs/align"
    with pytest.raises(AirflowException):
        op.execute_complete({}, {"exit_code": 2})


def test_instance_type_from_cpus_mem(monkeypatch):
    seen = {}

    def fake_pick(**kw):
        seen.update(kw)
        return "c6i.4xlarge"

    monkeypatch.setattr("spawn_airflow.operator.sizing.pick_instance_type", fake_pick)
    op = _op(cpus=8, memory_gib=32)
    assert op._instance_type() == "c6i.4xlarge"
    assert seen["cores"] == 8
    assert seen["mem_mb"] == 32 * 1024


def test_on_kill_terminates(monkeypatch):
    calls = []
    monkeypatch.setattr(
        SpawnRunTaskOperator, "_run_argv", lambda self, argv, check: calls.append(list(argv))
    )
    op = _op()
    op._instance_name = "af-align-1"
    op.on_kill()
    assert calls and calls[0] == ["spawn", "terminate", "af-align-1", "--yes"]


def test_trigger_serialize_roundtrip_and_run(monkeypatch):
    trig = SpawnExitCodeTrigger("s3://b/runs/align", "us-east-1", poll_interval=0)
    classpath, kwargs = trig.serialize()
    assert classpath.endswith("SpawnExitCodeTrigger")
    assert kwargs == {"s3_prefix": "s3://b/runs/align", "region": "us-east-1", "poll_interval": 0}

    # run(): first probe missing, then present -> one TriggerEvent(exit_code=0)
    states = [FakeCompleted(1, ""), FakeCompleted(0, "0\n")]
    monkeypatch.setattr(trig, "_probe", lambda: states.pop(0))

    async def drain():
        return [ev async for ev in trig.run()]

    events = asyncio.run(drain())
    assert len(events) == 1
    assert events[0].payload["exit_code"] == 0
