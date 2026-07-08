"""Build the per-task user-data script that runs on the ephemeral instance.

The Airflow operator hands us a shell ``command`` to run. The node script runs
it, capturing stdout/stderr to files, then uploads ``stdout.txt``/``stderr.txt``
and — *last* — ``.exitcode`` to S3. The ``.exitcode`` object's presence is the
durable completion signal the operator/trigger polls (the instance self-terminates
on completion, so it can't be probed directly). File-level input/output is the
command's own responsibility (it can ``aws s3 cp``), mirroring how
``EcsRunTaskOperator`` leaves container I/O to the container.

All functions are pure string builders (no I/O), unit-tested without AWS.
"""

from __future__ import annotations

import shlex

# Where the task runs on the instance's EBS root (NOT /tmp, which is tmpfs on AL2023).
JOB_DIR = "/mnt/spawn_airflow_job"


def _q(s: str) -> str:
    return shlex.quote(s)


def build_user_data(*, workdir_s3: str, region: str, command: str) -> str:
    """Assemble the full user-data script. Pure.

    ``command`` is the shell run on the instance. ``workdir_s3`` is the per-task
    prefix where ``stdout.txt``/``stderr.txt``/``.exitcode`` are written; the aws
    CLI + spored ship on stock AL2023, so no install preamble is needed.
    """
    sb: list[str] = ["#!/bin/bash\n", "set -uo pipefail\n\n"]
    sb.append(f"WORKDIR_S3={_q(workdir_s3)}\n")
    sb.append(f"AWS_REGION={_q(region)}\n")
    sb.append(f"JD={JOB_DIR}\n\n")

    sb.append('sudo mkdir -p "${JD}"\n')
    sb.append('sudo chown -R "$(id -u):$(id -g)" "${JD}"\n')
    sb.append('cd "${JD}"\n\n')

    sb.append(': > "${JD}/stdout.txt"\n')
    sb.append(': > "${JD}/stderr.txt"\n\n')

    # Run the task command; capture the real exit code.
    sb.append("(\n")
    sb.append(command.rstrip() + "\n")
    sb.append(') > "${JD}/stdout.txt" 2> "${JD}/stderr.txt"\n')
    sb.append("TASK_RC=$?\n")
    sb.append('echo "${TASK_RC}" > "${JD}/.exitcode"\n\n')

    # Upload stdout/stderr FIRST, .exitcode LAST (durable completion signal).
    sb.append(
        'aws s3 cp "${JD}/stdout.txt" "${WORKDIR_S3}/stdout.txt" '
        '--region "${AWS_REGION}" --quiet\n'
    )
    sb.append(
        'aws s3 cp "${JD}/stderr.txt" "${WORKDIR_S3}/stderr.txt" '
        '--region "${AWS_REGION}" --quiet\n'
    )
    sb.append(
        'aws s3 cp "${JD}/.exitcode" "${WORKDIR_S3}/.exitcode" '
        '--region "${AWS_REGION}" --quiet\n\n'
    )

    # Signal completion so spored terminates the instance.
    sb.append('if [ "${TASK_RC}" -eq 0 ]; then S=success; else S=failed; fi\n')
    sb.append('spored complete --status "${S}" 2>/dev/null || touch /tmp/SPAWN_COMPLETE\n')
    return "".join(sb)
