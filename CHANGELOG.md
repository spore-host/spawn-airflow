# Changelog

All notable changes to **spawn-airflow** are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **spawn-airflow now dispatches each task through `spawn task run`** instead of
  orchestrating the launch itself (spawn#386 adapter migration). `execute` builds
  a spawn **TaskSpec** and runs `spawn task run` (detached), then polls
  `spawn task status --check-complete` (sync) or defers to `SpawnTaskStatusTrigger`
  (deferrable), reading the exit code from the **CompletionRecord**. spawn now owns
  instance sizing (truffle), S3 staging, the durable completion record, and a
  **scoped least-privilege IAM profile** (was `--iam-policy s3:FullAccess`) — so
  spawn-airflow no longer reimplements any of it.
- **`SpawnExitCodeTrigger` is renamed `SpawnTaskStatusTrigger`** and now polls
  `spawn task status` instead of the raw `.exitcode` S3 object. Its serialized
  kwargs changed from `{s3_prefix, region, poll_interval}` to
  `{task_id, region, poll_interval}`.
- **The `instance_type` argument now steers the instance _family_** (e.g.
  `c7i.4xlarge` → the `c7i` family) rather than pinning the exact type; spawn's
  sizer picks the cheapest fit within it. (Exact-pin support is tracked as a spawn
  TaskSpec follow-up.)
- **The on-instance job dir moved to `/var/tmp/spawn_airflow_job`** (was
  `/mnt/spawn_airflow_job`): spawn runs the command as the instance's unprivileged
  login user, which can't create dirs under the root-owned `/mnt`.
- `truffle` is no longer required on `PATH` (spawn sizes the instance itself);
  `spawn` and AWS credentials are still required.

### Removed
- Bundled launch/completion/bootstrap/sizing machinery (`launch.py`,
  `completion.py`, `bootstrap.py`, `sizing.py`) — spawn owns these now.

## [0.1.0] - 2026-07-07

### Added
- Initial release: `SpawnRunTaskOperator` — an Apache Airflow operator that runs a
  task's command on an ephemeral EC2 instance via spore-host/spawn, auto-sized
  from `cpus`/`memory_gib` via truffle, launched with a TTL and `--on-complete
  terminate`, with a durable `.exitcode`-in-S3 completion signal. Deferrable (frees
  the worker slot while the instance runs) via `SpawnExitCodeTrigger`. The Airflow
  sibling of nf-spawn, miniwdl-spawn, cwl-spawn, and snakemake-executor-plugin-spawn.
- Verified end-to-end on real AWS: a `SpawnRunTaskOperator` task ran its command on
  a spawned EC2 instance (output captured to S3), surfaced exit 0, and the instance
  self-terminated (leak-checked clean).

[Unreleased]: https://github.com/spore-host/spawn-airflow/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/spore-host/spawn-airflow/releases/tag/v0.1.0
