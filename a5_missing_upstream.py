"""A5: upstream file missing in GCS.

Downloads an object that is never written by estate.sh, so the GCS client
raises a genuine NotFound. The RCA should point at the missing upstream object.
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.api_core.exceptions import NotFound
from google.cloud import storage

BUCKET = os.environ["PHQ_E2E_BUCKET"]
OBJECT = "data/a5_upstream_2026-10-31.csv"


def read_upstream_file() -> str:
    client = storage.Client()
    blob = client.bucket(BUCKET).blob(OBJECT)
    try:
        return blob.download_as_text()
    except NotFound as exc:
        raise FileNotFoundError(f"upstream object gs://{BUCKET}/{OBJECT} does not exist") from exc


with DAG(
    dag_id="a5_missing_upstream",
    description="A5: fails because the upstream GCS object was never produced",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["phq-e2e", "A5"],
) as dag:
    PythonOperator(task_id="read_upstream_file", python_callable=read_upstream_file)
