# spawn-airflow

The **ephemeral-EC2 operator for Apache Airflow** — run a task on a
purpose-sized EC2 instance via [spore-host/spawn](https://github.com/spore-host/spawn)
that auto-terminates when it's done. No cluster, no standing compute: the sibling
of [nf-spawn](https://github.com/spore-host/nf-spawn),
[miniwdl-spawn](https://github.com/spore-host/miniwdl-spawn),
[cwl-spawn](https://github.com/spore-host/cwl-spawn), and
[snakemake-executor-plugin-spawn](https://github.com/spore-host/snakemake-executor-plugin-spawn).

Each task's instance is auto-sized (via `truffle`), launched with a TTL and
`--on-complete terminate`, and torn down on completion — so it costs only the
compute the task actually uses. Deferrable, so a wide fan-out DAG doesn't pin a
worker slot per in-flight instance.

## Install

```bash
pip install spawn-airflow
```

Requires the `spawn` CLI on `PATH` (on the Airflow worker/triggerer) and AWS
credentials. spawn sizes the instance via truffle itself — no separate `truffle`
CLI needed.

## Use

```python
from spawn_airflow import SpawnRunTaskOperator

run = SpawnRunTaskOperator(
    task_id="align",
    command="aws s3 cp s3://in/sample.bam . && ./align sample.bam && aws s3 cp out/ s3://out/ --recursive",
    workdir_s3="s3://my-bucket/airflow-runs/{{ ds }}/align",
    cpus=8,
    memory_gib=32,          # -> cheapest fitting instance via truffle
    ttl="4h",
    spot=True,
    deferrable=True,        # frees the worker slot while the instance runs
)
```

The operator builds a spawn **TaskSpec** and dispatches `spawn task run`
(detached); spawn sizes and launches an instance that runs `command`, captures
stdout/stderr to `workdir_s3`, writes a durable completion record, and
self-terminates. The operator polls `spawn task status` (or defers to a trigger)
and fails the task on a nonzero exit. File-level I/O is the command's job (it can
`aws s3 cp`), exactly as `EcsRunTaskOperator` leaves container I/O to the
container. `instance_type` steers the instance _family_ (spawn picks the cheapest
fit within it), rather than pinning an exact type.

### Why not ECS / Batch?

- **truffle auto-sizing** — declare `cpus`/`memory_gib` (or nothing) and get the
  cheapest fitting instance; no task-defs or compute environments to pre-provision.
- **No cluster / control plane** — a bare EC2 instance that self-terminates; no
  ECS cluster or Batch queue to run and pay idle for.
- **Spot + TTL first-class**, and full-VM workloads (GPU/EFA/FSx/large-EBS) that
  don't want to be containerized. If your task is a tidy container, Batch is fine.

## License

Apache-2.0
