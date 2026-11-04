"""A2: bad conf.run_date format.

Reads `run_date` from the triggering conf and parses it as ISO date. A missing
or malformed value raises a genuine ValueError. `estate.sh fire a2` triggers
with a bad value; the corrected re-run passes conf `{"run_date": "2026-10-30"}`.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def parse_run_date(**context) -> str:
    conf = context["dag_run"].conf or {}
    raw = conf.get("run_date")
    if raw is None:
        raise ValueError("conf.run_date is required and was not provided")
    # Genuine parse failure on a malformed value, e.g. "not-a-date" or "31-10-2026".
    parsed = datetime.strptime(raw, "%Y-%m-%d")
    return parsed.date().isoformat()


with DAG(
    dag_id="a2_bad_run_date",
    description="A2: fails when conf.run_date is missing or not YYYY-MM-DD",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["phq-e2e", "A2"],
) as dag:
    PythonOperator(task_id="parse_run_date", python_callable=parse_run_date)
