"""The finance daily load: a partner file that never arrived (story act `missing-file`).

Downloads an object under `data/partner/` that is never written by estate.sh, so the GCS client
raises a genuine NotFound. The RCA should point at the missing upstream object and the partner
feed that was supposed to produce it -- there is no producer run for it anywhere in the estate.
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.api_core.exceptions import NotFound
from google.cloud import storage

BUCKET = os.environ["PHQ_E2E_BUCKET"]
OBJECT = "data/partner/shipment_feed_2026-11-05.csv"


def read_partner_file() -> str:
    client = storage.Client()
    blob = client.bucket(BUCKET).blob(OBJECT)
    try:
        return blob.download_as_text()
    except NotFound as exc:
        raise FileNotFoundError(
            f"partner file gs://{BUCKET}/{OBJECT} does not exist: no upstream run produced it"
        ) from exc


with DAG(
    dag_id="finance_daily_load",
    description="Loads the daily finance feed; fails because the partner file never landed",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["phq-e2e", "missing-file"],
) as dag:
    PythonOperator(task_id="read_partner_file", python_callable=read_partner_file)
