from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator,
)


PROJECT_ID = "retailpulse-lab-poc"
REGION = "asia-south1"

DATA_BUCKET = "ecommerce-data-bt"
TEMP_BUCKET = f"gs://{DATA_BUCKET}/temp"

DATAFLOW_TEMPLATE = (
    f"gs://dataflow-templates-{REGION}/latest/GCS_CSV_to_BigQuery"
)


with DAG(
    dag_id="retailpulse_batch_pipeline",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=["retailpulse", "batch", "dataflow"],
) as dag:

    batch_users = DataflowTemplatedJobStartOperator(
        task_id="batch_users",
        project_id=PROJECT_ID,
        location=REGION,
        template=DATAFLOW_TEMPLATE,
        job_name="retailpulse-users-{{ ts_nodash | lower }}",
        wait_until_finished=True,
        parameters={
            "inputFilePattern": (
                f"gs://{DATA_BUCKET}/incoming/users/*.csv"
            ),
            "schemaJSONPath": (
                f"gs://{DATA_BUCKET}/schema/raw_users_schema.json"
            ),
            "outputTable": (
                f"{PROJECT_ID}:ecommerce_bronze.raw_users"
            ),
            "bigQueryLoadingTemporaryDirectory": TEMP_BUCKET,
            "badRecordsOutputTable": (
                f"{PROJECT_ID}:ecommerce_bronze.bad_users"
            ),
            "delimiter": ",",
            "csvFormat": "Default",
            "containsHeaders": "true",
            "csvFileEncoding": "UTF-8",
        },
    )

    batch_products = DataflowTemplatedJobStartOperator(
        task_id="batch_products",
        project_id=PROJECT_ID,
        location=REGION,
        template=DATAFLOW_TEMPLATE,
        job_name="retailpulse-products-{{ ts_nodash | lower }}",
        wait_until_finished=True,
        parameters={
            "inputFilePattern": (
                f"gs://{DATA_BUCKET}/incoming/products/*.csv"
            ),
            "schemaJSONPath": (
                f"gs://{DATA_BUCKET}/schema/raw_products_schema.json"
            ),
            "outputTable": (
                f"{PROJECT_ID}:ecommerce_bronze.raw_products"
            ),
            "bigQueryLoadingTemporaryDirectory": TEMP_BUCKET,
            "badRecordsOutputTable": (
                f"{PROJECT_ID}:ecommerce_bronze.bad_products"
            ),
            "delimiter": ",",
            "csvFormat": "Default",
            "containsHeaders": "true",
            "csvFileEncoding": "UTF-8",
        },
    )

    batch_orders = DataflowTemplatedJobStartOperator(
        task_id="batch_orders",
        project_id=PROJECT_ID,
        location=REGION,
        template=DATAFLOW_TEMPLATE,
        job_name="retailpulse-orders-{{ ts_nodash | lower }}",
        wait_until_finished=True,
        parameters={
            "inputFilePattern": (
                f"gs://{DATA_BUCKET}/incoming/orders/*.csv"
            ),
            "schemaJSONPath": (
                f"gs://{DATA_BUCKET}/schema/raw_orders_schema.json"
            ),
            "outputTable": (
                f"{PROJECT_ID}:ecommerce_bronze.raw_orders"
            ),
            "bigQueryLoadingTemporaryDirectory": TEMP_BUCKET,
            "badRecordsOutputTable": (
                f"{PROJECT_ID}:ecommerce_bronze.bad_orders"
            ),
            "delimiter": ",",
            "csvFormat": "Default",
            "containsHeaders": "true",
            "csvFileEncoding": "UTF-8",
        },
    )

    batch_order_items = DataflowTemplatedJobStartOperator(
        task_id="batch_order_items",
        project_id=PROJECT_ID,
        location=REGION,
        template=DATAFLOW_TEMPLATE,
        job_name="retailpulse-order-items-{{ ts_nodash | lower }}",
        wait_until_finished=True,
        parameters={
            "inputFilePattern": (
                f"gs://{DATA_BUCKET}/incoming/order_items/*.csv"
            ),
            "schemaJSONPath": (
                f"gs://{DATA_BUCKET}/schema/raw_order_items_schema.json"
            ),
            "outputTable": (
                f"{PROJECT_ID}:ecommerce_bronze.raw_order_items"
            ),
            "bigQueryLoadingTemporaryDirectory": TEMP_BUCKET,
            "badRecordsOutputTable": (
                f"{PROJECT_ID}:ecommerce_bronze.bad_order_items"
            ),
            "delimiter": ",",
            "csvFormat": "Default",
            "containsHeaders": "true",
            "csvFileEncoding": "UTF-8",
        },
    )
