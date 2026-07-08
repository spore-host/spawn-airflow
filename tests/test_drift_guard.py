"""Drift guard: pin the Airflow public seams the operator/trigger rely on. These
are the most stable Airflow API (unchanged across the 2->3 transition), but a
signature move should fail loudly here, not in a live DAG. See CLAUDE.md.
"""

import inspect


def test_base_operator_execute_and_defer():
    from airflow.sdk.bases.operator import BaseOperator

    exec_params = inspect.signature(BaseOperator.execute).parameters
    assert "context" in exec_params

    defer_params = inspect.signature(BaseOperator.defer).parameters
    for p in ("trigger", "method_name", "kwargs", "timeout"):
        assert p in defer_params, f"BaseOperator.defer lost '{p}'"


def test_base_trigger_run_and_serialize():
    from airflow.triggers.base import BaseTrigger

    assert BaseTrigger.__abstractmethods__ == frozenset({"run", "serialize"})
    assert "self" in inspect.signature(BaseTrigger.run).parameters


def test_conf_getboolean_available():
    from airflow.configuration import conf

    assert hasattr(conf, "getboolean")


def test_trigger_event_payload():
    from airflow.triggers.base import TriggerEvent

    ev = TriggerEvent({"exit_code": 0})
    assert ev.payload == {"exit_code": 0}
