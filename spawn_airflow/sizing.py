"""Map a Snakemake job's resources to an EC2 instance type.

Snakemake jobs declare ``threads`` and ``resources`` (``mem_mb`` in megabytes,
plus ``disk_mb`` etc.), so — like WDL/CWL — we can auto-pick the cheapest instance
that fits via truffle. This module turns ``threads``/``mem_mb`` into a ``truffle
search`` query. An explicit per-job override (an ``instance_type`` ExecutorSettings
value or resource) always wins; if truffle is unavailable we fall back to a
configurable default. Pure except for the one subprocess call to truffle
(injected for tests).
"""

from __future__ import annotations

import math
import shutil
import subprocess
from typing import Callable, Optional

# Default when neither an explicit type nor a successful truffle lookup is available.
DEFAULT_INSTANCE_TYPE = "t3.medium"


def mb_to_gib(mem_mb: object) -> Optional[float]:
    """Coerce Snakemake's ``resources.mem_mb`` (megabytes) to GiB.

    Snakemake ``mem_mb`` is a number of megabytes. We convert to GiB for truffle's
    ``--min-memory`` (which rounds up), so a slightly conservative MB→GiB (/1024)
    never under-provisions. Returns None if missing/unparseable (caller omits the
    --min-memory filter).
    """
    if mem_mb is None:
        return None
    try:
        val = float(mem_mb)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if val <= 0:
        return None
    return val / 1024.0  # MB -> ~GiB (conservative; truffle rounds up)


def build_truffle_argv(
    min_vcpu: Optional[int], min_memory_gib: Optional[float], architecture: Optional[str]
) -> list[str]:
    """Build the ``truffle search`` argv that returns the cheapest fitting type.

    ``--pick-first`` makes truffle emit only the top result's instance type;
    ``--show-price`` makes the default sort cheapest-first. Pure.
    """
    argv = ["truffle", "search", "--pick-first", "--show-price"]
    if min_vcpu and min_vcpu > 0:
        argv += ["--min-vcpu", str(int(min_vcpu))]
    if min_memory_gib and min_memory_gib > 0:
        # Round up so we never under-provision a fractional GiB request.
        argv += ["--min-memory", str(int(math.ceil(min_memory_gib)))]
    if architecture:
        argv += ["--architecture", architecture]
    return argv


def pick_instance_type(
    *,
    override: Optional[str] = None,
    cores: Optional[int] = None,
    mem_mb: object = None,
    architecture: Optional[str] = None,
    default: str = DEFAULT_INSTANCE_TYPE,
    runner: Optional[Callable[[list[str]], str]] = None,
) -> str:
    """Resolve the instance type for a job.

    Precedence: ``override`` (an explicit instance type) > truffle cheapest-fit
    (from threads/mem_mb) > ``default``. ``runner`` runs the truffle argv and
    returns its stdout (injected in tests); when None, a real subprocess is used
    iff truffle is on PATH.
    """
    if override:
        return override.strip()

    min_mem_gib = mb_to_gib(mem_mb)
    if (cores is None or cores <= 0) and min_mem_gib is None:
        return default  # nothing to size on

    argv = build_truffle_argv(cores, min_mem_gib, architecture)

    if runner is None:
        if shutil.which("truffle") is None:
            return default

        def runner(a: list[str]) -> str:
            return subprocess.run(
                a, capture_output=True, text=True, timeout=120, check=True
            ).stdout

    try:
        out = runner(argv)
    except Exception:
        return default
    for line in (out or "").strip().splitlines():
        line = line.strip()
        if line:
            return line
    return default
