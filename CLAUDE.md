# CLAUDE.md — spawn-airflow

An **Apache Airflow operator** that runs a task on an ephemeral EC2 instance via
[spore-host/spawn](https://github.com/spore-host/spawn). The Airflow sibling of
`nf-spawn` (Nextflow), `miniwdl-spawn` (WDL), `cwl-spawn` (CWL), and
`snakemake-executor-plugin-spawn` (Snakemake). Part of the spore.host suite.

## Versioning & changelog (required)

Follows **[Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html)** and keeps a
**[Keep a Changelog](https://keepachangelog.com/en/1.1.0/)**-format `CHANGELOG.md`
(spore.host-wide policy).

**Every user-facing change updates `CHANGELOG.md`** in the same PR under
`## [Unreleased]` (Added/Changed/Deprecated/Removed/Fixed/Security).

**On release:** rename `## [Unreleased]` → `## [X.Y.Z] - YYYY-MM-DD`; bump
`version` in `pyproject.toml` to match (the release workflow fails on drift); tag
`vX.Y.Z`.

## Build & test

Python package (3.10+). Needs the `spawn` and `truffle` CLIs on PATH at runtime,
plus AWS credentials, for real runs.

- `pip install -e ".[dev]"` — install with dev deps
- `pytest` — pure-function + operator/trigger unit tests (no AWS, no scheduler)
- `ruff check .` && `mypy spawn_airflow` — lint + type-check

## Architecture — Operator, NOT Executor

Airflow is not a per-run CLI engine, so there's no execution-backend to replace.
The idiomatic seam is a custom **Operator** (opt-in, per-task, zero deployment
changes), NOT a custom Executor (deployment-wide, churned 2→3, wrong granularity).

- `operator.py` — `SpawnRunTaskOperator(BaseOperator)`; `execute` launches a
  spawn instance and either polls (sync) or `defer`s to the trigger; on failure/
  `on_kill` it `spawn terminate`s.
- `trigger.py` — `SpawnExitCodeTrigger(BaseTrigger)`; async-polls the durable
  `.exitcode` in S3 (frees the worker slot). Reuses `completion.py` via
  `asyncio.to_thread` (no aioboto3 dep).
- `launch.py` / `completion.py` / `sizing.py` / `bootstrap.py` — **pure** helpers
  (no I/O), unit-tested. Ported from the sibling packages.

Import `BaseOperator`/`BaseTrigger` from their stable public locations
(`airflow.sdk.BaseOperator` in 3.x, `airflow.models.baseoperator` fallback in
2.x). Mirror `EcsRunTaskOperator` (submit → `if deferrable: defer` else poll →
`execute_complete`).

## Cost safety

Real runs launch billable EC2 instances. Any real-AWS test MUST set a TTL,
terminate explicitly, and leak-check afterward. The operator always launches with
`--on-complete terminate` and a TTL.

## Gotcha

Do NOT `from __future__ import annotations` in a module whose class annotations
Airflow reads at runtime (bit the Snakemake plugin's settings dataclass). The
pure helpers are fine; be careful in operator.py if adding annotated params
Airflow introspects.
