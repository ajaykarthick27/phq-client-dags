"""A3: a genuine code bug in the DAG.

`conf["expected_key"]` is never set by any real trigger payload, so this is a
real KeyError raised by the task body, not a simulated one. Phase 2/3 opens a
fix PR against this file on the DAG source repo (decision D3 pending as of
Phase 0) once the libcst fix-router lane is wired up.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def buggy_task(**context) -> str:
    conf = context["dag_run"].conf or {}
    # Bug: this key is never populated by callers. KeyError is the real failure.
    value = conf["expected_key"]
    return value


with DAG(
    dag_id="a3_code_bug",
    description="A3: KeyError from a DAG code bug, needs a fix PR",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["phq-e2e", "A3"],
) as dag:
    PythonOperator(task_id="buggy_task", python_callable=buggy_task)
