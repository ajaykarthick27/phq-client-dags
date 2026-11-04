"""A4: a real data-quality check over a CSV in GCS.

Downloads data/a4_customers.csv (uploaded by estate.sh) and computes the null
rate on the `email` column and the row count. Fails when the null rate exceeds
10% or the row count is below 20, the real thresholds for this check, not a
simulated flag.
"""

import csv
import io
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage

BUCKET = os.environ["PHQ_E2E_BUCKET"]
OBJECT = "data/a4_customers.csv"
NULL_RATE_THRESHOLD = 0.10
MIN_ROW_COUNT = 20


def check_data_quality() -> dict:
    client = storage.Client()
    blob = client.bucket(BUCKET).blob(OBJECT)
    raw = blob.download_as_text()
    rows = list(csv.DictReader(io.StringIO(raw)))
    row_count = len(rows)
    null_count = sum(1 for r in rows if not r.get("email"))
    null_rate = null_count / row_count if row_count else 1.0

    failures = []
    if row_count < MIN_ROW_COUNT:
        failures.append(f"row_count={row_count} < {MIN_ROW_COUNT}")
    if null_rate > NULL_RATE_THRESHOLD:
        failures.append(f"email null_rate={null_rate:.2%} > {NULL_RATE_THRESHOLD:.0%}")
    if failures:
        raise ValueError(f"data quality check failed on {OBJECT}: {'; '.join(failures)}")
    return {"row_count": row_count, "null_rate": null_rate}


with DAG(
    dag_id="a4_data_quality",
    description="A4: null-rate and row-count check over a GCS CSV",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["phq-e2e", "A4"],
) as dag:
    PythonOperator(task_id="check_data_quality", python_callable=check_data_quality)
