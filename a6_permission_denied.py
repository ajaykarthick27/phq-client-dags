"""A6: permission denied on the bucket's restricted/ prefix.

The attached VM service account (phq-e2e-client@...) has a conditional IAM
binding that excludes objects under restricted/. Reading from that prefix is a
genuine 403, proven independently via scripts/e2e/estate.sh status.
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.api_core.exceptions import Forbidden
from google.cloud import storage

BUCKET = os.environ["PHQ_E2E_BUCKET"]
OBJECT = "restricted/.keep"


def read_restricted_object() -> str:
    client = storage.Client()
    blob = client.bucket(BUCKET).blob(OBJECT)
    try:
        return blob.download_as_text()
    except Forbidden as exc:
        raise PermissionError(f"access denied reading gs://{BUCKET}/{OBJECT}") from exc


with DAG(
    dag_id="a6_permission_denied",
    description="A6: non-retryable permission denial on a restricted GCS prefix",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["phq-e2e", "A6"],
) as dag:
    PythonOperator(task_id="read_restricted_object", python_callable=read_restricted_object)
