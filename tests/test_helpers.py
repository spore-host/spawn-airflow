from spawn_airflow.bootstrap import JOB_DIR, build_user_data
from spawn_airflow.completion import (
    build_exitcode_probe_argv,
    exitcode_uri,
    parse_exit_code,
)
from spawn_airflow.launch import LaunchSpec, build_cancel_argv, build_launch_argv
from spawn_airflow.sizing import DEFAULT_INSTANCE_TYPE, mb_to_gib, pick_instance_type


# ---- launch ----------------------------------------------------------------

def test_launch_argv_core():
    argv = build_launch_argv(
        LaunchSpec(name="af-x", instance_type="c7g.2xlarge", region="us-east-1",
                   user_data_file="/tmp/s.sh", ttl="4h", spot=True)
    )
    assert argv[:3] == ["spawn", "launch", "af-x"]
    assert argv[argv.index("--on-complete") + 1] == "terminate"
    assert "--user-data-file" in argv and "--user-data" not in argv
    assert "--wait-for-running=false" in argv and "-y" in argv and "--spot" in argv
    assert argv[argv.index("--iam-policy") + 1] == "s3:FullAccess"


def test_cancel_argv_has_no_region_flag():
    # `spawn terminate` resolves by name/id and takes no --region.
    assert build_cancel_argv("af-x") == ["spawn", "terminate", "af-x", "--yes"]


# ---- completion ------------------------------------------------------------

def test_completion():
    assert exitcode_uri("s3://b/p") == "s3://b/p/.exitcode"
    assert build_exitcode_probe_argv("s3://b/p", "us-west-2")[-1] == "us-west-2"
    assert parse_exit_code("0\n") == 0
    assert parse_exit_code("137") == 137
    assert parse_exit_code(None) is None
    assert parse_exit_code("") is None


# ---- sizing ----------------------------------------------------------------

def test_sizing():
    assert mb_to_gib(2048) == 2.0
    assert mb_to_gib(None) is None
    assert pick_instance_type(override=" c7g.8xlarge ", cores=1) == "c7g.8xlarge"
    assert pick_instance_type() == DEFAULT_INSTANCE_TYPE

    def fake(argv):
        assert "--min-vcpu" in argv
        return "c6i.4xlarge\n"

    assert pick_instance_type(cores=16, mem_mb=32000, runner=fake) == "c6i.4xlarge"


# ---- bootstrap -------------------------------------------------------------

def test_user_data_runs_command_then_exitcode_last():
    ud = build_user_data(
        workdir_s3="s3://b/runs/t", region="us-east-1",
        command="echo hello, spore",
    )
    assert ud.startswith("#!/bin/bash\n")
    assert JOB_DIR in ud
    assert "echo hello, spore" in ud
    # exit code captured, then stdout/stderr uploaded, then .exitcode LAST
    i_rc = ud.index("TASK_RC=$?")
    i_out = ud.index('cp "${JD}/stdout.txt"')
    i_exit = ud.index('cp "${JD}/.exitcode"')
    assert i_rc < i_out < i_exit
    assert ud.rindex("SPAWN_COMPLETE") > i_exit or ud.rindex("spored complete") > i_exit
