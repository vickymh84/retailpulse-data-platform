from datetime import datetime, timezone
import hashlib
import re
import uuid

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
    max_active_runs=1,
    tags=[
        "retailpulse",
        "batch",
        "gcp",
    ],
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

        # --------------------------------------------------
        # Find files already processed successfully
        # --------------------------------------------------

        query = f"""
        SELECT DISTINCT file_path
        FROM `{AUDIT_TABLE}`
        WHERE status = 'SUCCESS'
        """

        processed_files = {
            row.file_path
            for row in bq_client.query(query).result()
        }

        # --------------------------------------------------
        # Discover new CSV files
        # --------------------------------------------------

        dataflow_jobs = []

        run_timestamp = datetime.now(
            timezone.utc
        ).strftime("%Y%m%d%H%M%S")

        for entity, config in ENTITY_CONFIG.items():

            prefix = f"incoming/{entity}/"

            blobs = storage_client.list_blobs(
                DATA_BUCKET,
                prefix=prefix,
            )

            for blob in blobs:

                # Only process CSV files
                if not blob.name.endswith(".csv"):
                    continue

                file_path = (
                    f"gs://{DATA_BUCKET}/{blob.name}"
                )

                # Skip files already processed
                if file_path in processed_files:
                    continue

                # --------------------------------------------------
                # Create unique Dataflow job name
                # --------------------------------------------------

                file_hash = hashlib.md5(
                    file_path.encode("utf-8")
                ).hexdigest()[:8]

                job_name = (
                    f"rp-{entity}-"
                    f"{run_timestamp}-"
                    f"{file_hash}"
                )

                job_name = re.sub(
                    r"[^a-z0-9-]",
                    "-",
                    job_name.lower(),
                )

                # --------------------------------------------------
                # Dataflow job definition
                # --------------------------------------------------

                dataflow_jobs.append(
                    {
                        "job_name": job_name,

                        "parameters": {

                            "inputFilePattern": file_path,

                            "schemaJSONPath": (
                                f"gs://{DATA_BUCKET}/schema/"
                                f"{config['schema']}"
                            ),

                            "outputTable": (
                                f"{PROJECT_ID}:"
                                f"ecommerce_bronze."
                                f"{config['table']}"
                            ),

                            "bigQueryLoadingTemporaryDirectory": (
                                f"gs://{DATA_BUCKET}/temp/"
                            ),

                            "badRecordsOutputTable": (
                                f"{PROJECT_ID}:"
                                f"ecommerce_bronze."
                                f"{config['bad_table']}"
                            ),

                            "delimiter": ",",

                            "csvFormat": "Default",

                            "containsHeaders": "true",
                        },
                    }
                )

        print(
            f"New files discovered: "
            f"{len(dataflow_jobs)}"
        )

        for job in dataflow_jobs:
            print(
                f"Dataflow job: "
                f"{job['job_name']}"
            )

            print(
                f"Input: "
                f"{job['parameters']['inputFilePattern']}"
            )

        # IMPORTANT:
        # Return the LIST directly.
        # Do NOT return {"dataflow": ...}
        return dataflow_jobs


    # ------------------------------------------------------
    # Discover new files
    # ------------------------------------------------------

    new_files = check_new_files()


    # ------------------------------------------------------
    # Dynamically create one Dataflow task per file
    # ------------------------------------------------------

    run_dataflow = (
        DataflowTemplatedJobStartOperator.partial(
            task_id="run_dataflow",

            project_id=PROJECT_ID,

            location=REGION,

            template=DATAFLOW_TEMPLATE,

            wait_until_finished=True,
        )
        .expand_kwargs(new_files)
    )


    # ------------------------------------------------------
    # Record successful ingestion
    # ------------------------------------------------------

    @task
    def record_ingestion_success(file_info):

        from google.cloud import storage
        from google.cloud import bigquery

        storage_client = storage.Client(
            project=PROJECT_ID
        )

        bq_client = bigquery.Client(
            project=PROJECT_ID
        )

        file_path = file_info[
            "parameters"
        ][
            "inputFilePattern"
        ]

        job_name = file_info[
            "job_name"
        ]

        # --------------------------------------------------
        # Get GCS object metadata
        # --------------------------------------------------

        path_without_prefix = file_path[
            f"gs://{DATA_BUCKET}/"
            .__len__():
        ]

        blob = storage_client.bucket(
            DATA_BUCKET
        ).get_blob(
            path_without_prefix
        )

        file_size = None
        file_generation = None
        file_name = path_without_prefix.split("/")[-1]

        if blob:
            file_size = blob.size
            file_generation = str(
                blob.generation
            )

        now = datetime.now(
            timezone.utc
        ).isoformat()

        # --------------------------------------------------
        # Determine entity
        # --------------------------------------------------

        for entity, config in ENTITY_CONFIG.items():

            if (
                f"incoming/{entity}/"
                in path_without_prefix
            ):
                entity_name = entity
                break
        else:
            entity_name = "unknown"

        # --------------------------------------------------
        # Create audit record
        # --------------------------------------------------

        row = {

            "audit_id": str(
                uuid.uuid4()
            ),

            "file_name": file_name,

            "file_path": file_path,

            "entity_name": entity_name,

            "batch_id": job_name,

            "file_size_bytes": file_size,

            "file_generation": file_generation,

            "started_at": now,

            "completed_at": datetime.now(
                timezone.utc
            ).isoformat(),

            "status": "SUCCESS",

            "row_count": None,

            "bad_row_count": None,

            "dataflow_job_name": job_name,

            "error_message": None,

            "created_at": datetime.now(
                timezone.utc
            ).isoformat(),
        }

        errors = bq_client.insert_rows_json(
            AUDIT_TABLE,
            [row],
            row_ids=[
                row["audit_id"]
            ],
        )

        if errors:
            raise RuntimeError(
                f"Failed to write ingestion "
                f"audit: {errors}"
            )

        print(
            f"SUCCESS audit recorded: "
            f"{file_path}"
        )


    record_success = (
        record_ingestion_success
        .expand(
            file_info=new_files
        )
    )


    # ------------------------------------------------------
    # Dependency
    # ------------------------------------------------------

    run_dataflow >> record_success
