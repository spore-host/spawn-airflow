"""spawn-airflow: run an Airflow task on an ephemeral EC2 instance via
spore-host/spawn. The ephemeral-EC2 operator for Airflow — no cluster,
truffle-auto-sized, spot-capable, self-terminating.
"""

from .operator import SpawnRunTaskOperator
from .trigger import SpawnExitCodeTrigger

__version__ = "0.1.0"

__all__ = ["SpawnRunTaskOperator", "SpawnExitCodeTrigger"]
