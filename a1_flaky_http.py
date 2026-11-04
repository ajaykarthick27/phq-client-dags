"""A1: flaky HTTP task.

The task issues a real HTTP GET against the GCS JSON API for a marker object.
Absent the marker, GCS genuinely returns 404 and the task fails. `estate.sh fire
a1-pass` writes the marker and retriggers the DAG, at which point the same GET
genuinely returns 200. retries=0 so PHQ observes the raw failure on try 1.
"""

import os
from datetime import datetime

import google.auth
import google.auth.transport.requests
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

BUCKET = os.environ["PHQ_E2E_BUCKET"]
MARKER_OBJECT = "data/a1-pass.marker"


def check_marker_http() -> dict:
    creds, _ = google.auth.default()
    creds.refresh(google.auth.transport.requests.Request())
    encoded = MARKER_OBJECT.replace("/", "%2F")
    url = f"https://storage.googleapis.com/storage/v1/b/{BUCKET}/o/{encoded}"
    resp = requests.get(url, headers={"Authorization": f"Bearer {creds.token}"}, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(
            f"flaky upstream check failed: GET {url} -> {resp.status_code} {resp.text}"
        )
    return resp.json()


with DAG(
    dag_id="a1_flaky_http",
    description="A1: flaky HTTP task, fails until the GCS marker exists",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["phq-e2e", "A1"],
) as dag:
    PythonOperator(task_id="check_upstream_http", python_callable=check_marker_http)
