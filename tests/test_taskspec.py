"""Unit tests for the pure TaskSpec builder + CompletionRecord parsing."""

import pytest

from spawn_airflow import taskspec

JD = "/var/tmp/spawn_airflow_job"


def _spec(**over):
    base = dict(
        task_id="af-align-1",
        command="echo hi",
        job_dir=JD,
        workdir_s3="s3://b/runs/align",
    )
    base.update(over)
    return taskspec.build_task_spec(**base)


def test_command_runs_in_job_dir_with_redirects():
    spec = _spec()
    cmd = spec["command"]
    assert cmd[0] == "/bin/bash" and cmd[1] == "-lc"
    inner = cmd[2]
    assert f"mkdir -p {JD}" in inner
    assert f"cd {JD}" in inner
    assert "> stdout.txt 2> stderr.txt" in inner
    assert "( echo hi )" in inner


def test_no_input_manifest():
    spec = _spec()
    assert "inputs" not in spec


def test_output_manifest_syncs_job_dir_back():
    spec = _spec()
    assert spec["outputs"] == [{"source": JD + "/", "destination": "s3://b/runs/align/"}]


def test_resources_from_cpus_and_memory():
    spec = _spec(cpus=8, memory_gib=32.0)
    assert spec["resources"]["cpu"] == 8
    assert spec["resources"]["memory_gib"] == pytest.approx(32.0)


def test_resources_omitted_when_absent():
    assert _spec()["resources"] == {}


def test_spot_maps_to_purchase_with_fallback():
    r = _spec(spot=True)["resources"]
    assert r["purchase"] == "spot" and r["fallback"] == "on_demand"


def test_instance_hint_maps_to_family():
    assert _spec(instance_hint="c6i.4xlarge")["resources"]["families"] == ["c6i"]


def test_lifecycle_defaults_terminate():
    assert _spec(ttl="2h")["lifecycle"] == {"ttl": "2h", "on_complete": "terminate"}


def test_no_container_key():
    # Airflow tasks have no container image concept in this operator.
    assert "container" not in _spec()


def test_instance_type_family_edges():
    assert taskspec.instance_type_family(None) is None
    assert taskspec.instance_type_family("junk") is None
    assert taskspec.instance_type_family("m7i.large") == "m7i"


def test_check_complete_to_status_contract():
    assert taskspec.check_complete_to_status(0) == "completed"
    assert taskspec.check_complete_to_status(1) == "failed"
    assert taskspec.check_complete_to_status(2) is None
    with pytest.raises(RuntimeError):
        taskspec.check_complete_to_status(3)


def test_parse_completion_record():
    rec = taskspec.parse_completion_record('{"exit_code":7,"state":"failed"}')
    assert rec["exit_code"] == 7
    with pytest.raises(Exception):
        taskspec.parse_completion_record("nope")
