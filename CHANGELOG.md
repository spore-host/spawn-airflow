# Changelog

All notable changes to **spawn-airflow** are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
