from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator,
)


PROJECT_ID = "retailpulse-lab-poc"
REGION = "asia-south1"

DATA_BUCKET = "ecommerce-data-bt"
AUDIT_TABLE = (
    f"{PROJECT_ID}.ecommerce_bronze.ingestion_audit"
)

DATAFLOW_TEMPLATE = (
    f"gs://dataflow-templates-{REGION}/latest/"
    "GCS_CSV_to_BigQuery"
)


with DAG(
    dag_id="retailpulse_batch_pipeline",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=["retailpulse", "batch", "dataflow"],
) as dag:

    @task
    def check_new_files():
        """
        Check GCS for CSV files that have not already
        been successfully processed.

        This is the first stage of file-level idempotency.
        """

        from google.cloud import storage
        from google.cloud import bigquery

        storage_client = storage.Client(
            project=PROJECT_ID
        )

        bq_client = bigquery.Client(
            project=PROJECT_ID
        )

        bucket = storage_client.bucket(DATA_BUCKET)

        query = f"""
            SELECT DISTINCT file_path
            FROM `{AUDIT_TABLE}`
            WHERE status = 'SUCCESS'
        """

        processed_files = {
            row.file_path
            for row in bq_client.query(query).result()
        }

        entities = [
            "users",
            "products",
            "orders",
            "order_items",
        ]

        new_files = {}

        for entity in entities:

            prefix = f"incoming/{entity}/"

            blobs = storage_client.list_blobs(
                bucket,
                prefix=prefix,
            )

            files = []

            for blob in blobs:

                if not blob.name.endswith(".csv"):
                    continue

                file_path = (
                    f"gs://{DATA_BUCKET}/{blob.name}"
                )

                if file_path not in processed_files:
                    files.append(file_path)

            new_files[entity] = files

        print(
            f"New files discovered: {new_files}"
        )

        return new_files


    new_files = check_new_files()


    @task
    def show_new_files(files):
        """
        Display discovered files in the Airflow logs.
        """

        print("Files selected for ingestion:")

        for entity, paths in files.items():

            print(f"\n{entity}:")

            for path in paths:
                print(f"  {path}")


    show_files = show_new_files(new_files)
