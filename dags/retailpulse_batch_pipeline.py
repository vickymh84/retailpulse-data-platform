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


ENTITY_CONFIG = {
    "users": {
        "schema": "raw_users_schema.json",
        "table": "raw_users",
        "bad_table": "bad_users",
    },
    "products": {
        "schema": "raw_products_schema.json",
        "table": "raw_products",
        "bad_table": "bad_products",
    },
    "orders": {
        "schema": "raw_orders_schema.json",
        "table": "raw_orders",
        "bad_table": "bad_orders",
    },
    "order_items": {
        "schema": "raw_order_items_schema.json",
        "table": "raw_order_items",
        "bad_table": "bad_order_items",
    },
}


with DAG(
    dag_id="retailpulse_batch_pipeline",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=["retailpulse", "batch", "dataflow"],
) as dag:

    @task
    def check_new_files():

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

        new_files = []

        for entity, config in ENTITY_CONFIG.items():

            prefix = f"incoming/{entity}/"

            blobs = storage_client.list_blobs(
                bucket,
                prefix=prefix,
            )

            for blob in blobs:

                if not blob.name.endswith(".csv"):
                    continue

                file_path = (
                    f"gs://{DATA_BUCKET}/{blob.name}"
                )

                if file_path in processed_files:
                    continue

                new_files.append(
                    {
                        "entity": entity,
                        "file_path": file_path,
                        "file_name": blob.name.split("/")[-1],
                        "schema": (
                            f"gs://{DATA_BUCKET}/schema/"
                            f"{config['schema']}"
                        ),
                        "output_table": (
                            f"{PROJECT_ID}:"
                            f"ecommerce_bronze."
                            f"{config['table']}"
                        ),
                        "bad_records_table": (
                            f"{PROJECT_ID}:"
                            f"ecommerce_bronze."
                            f"{config['bad_table']}"
                        ),
                    }
                )

        print(
            f"New files discovered: {new_files}"
        )

        return new_files


    new_files = check_new_files()


    run_dataflow = DataflowTemplatedJobStartOperator.partial(
        task_id="run_dataflow",
        project_id=PROJECT_ID,
        location=REGION,
        template=DATAFLOW_TEMPLATE,
        wait_until_finished=True,
    ).expand(
        parameters=new_files.map(
            lambda file: {
                "inputFilePattern": file["file_path"],
                "schemaJSONPath": file["schema"],
                "outputTable": file["output_table"],
                "bigQueryLoadingTemporaryDirectory": (
                    f"gs://{DATA_BUCKET}/temp"
                ),
                "badRecordsOutputTable": (
                    file["bad_records_table"]
                ),
                "delimiter": ",",
                "csvFormat": "Default",
                "containsHeaders": "true",
                "csvFileEncoding": "UTF-8",
            }
        )
    )

    check_new_files() >> run_dataflow
