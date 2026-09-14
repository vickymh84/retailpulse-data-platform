from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="retailpulse_batch_pipeline",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=["retailpulse", "batch", "gcp"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> end
