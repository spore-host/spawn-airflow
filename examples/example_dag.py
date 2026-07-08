"""Example Airflow DAG using spawn-airflow's SpawnRunTaskOperator.

Each task runs its command on a purpose-sized, ephemeral EC2 instance that
self-terminates. Drop this in your Airflow ``dags/`` folder (the `spawn` and
`truffle` CLIs must be on the worker/triggerer PATH, with AWS credentials).
"""

from __future__ import annotations

import pendulum
from airflow import DAG

from spawn_airflow import SpawnRunTaskOperator

with DAG(
    dag_id="spawn_airflow_example",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["spawn", "ephemeral-ec2"],
) as dag:
    hello = SpawnRunTaskOperator(
        task_id="hello",
        command="echo 'hello, spore'",
        workdir_s3="s3://YOUR-BUCKET/airflow-runs/{{ ds }}/hello",
        cpus=2,
        memory_gib=4,
        ttl="1h",
        deferrable=True,
    )

    align = SpawnRunTaskOperator(
        task_id="align",
        command=(
            "aws s3 cp s3://YOUR-BUCKET/in/sample.bam . "
            "&& echo 'pretend-align sample.bam' > out.txt "
            "&& aws s3 cp out.txt s3://YOUR-BUCKET/out/align.txt"
        ),
        workdir_s3="s3://YOUR-BUCKET/airflow-runs/{{ ds }}/align",
        cpus=8,
        memory_gib=32,
        ttl="4h",
        spot=True,
        deferrable=True,
    )

    hello >> align
