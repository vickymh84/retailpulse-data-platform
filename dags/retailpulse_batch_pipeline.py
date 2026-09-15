from datetime import datetime, timezone
import hashlib
import re
import uuid

from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator,
)


# ============================================================
# CONFIGURATION
# ============================================================

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


# ============================================================
# ENTITY CONFIGURATION
# ============================================================

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


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="retailpulse_batch_pipeline",

    start_date=datetime(
        2026,
        9,
        1
    ),

    schedule=None,

    catchup=False,

    max_active_runs=1,

    tags=[
        "retailpulse",
        "batch",
        "gcp",
    ],
) as dag:

    # ========================================================
    # 1. DISCOVER NEW FILES
    # ========================================================

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

        # ----------------------------------------------------
        # Get successfully processed files
        # ----------------------------------------------------

        query = f"""
        SELECT DISTINCT file_path
        FROM `{AUDIT_TABLE}`
        WHERE status = 'SUCCESS'
        """

        processed_files = {
            row.file_path
            for row in bq_client.query(query).result()
        }

        print(
            f"Already processed files: "
            f"{len(processed_files)}"
        )

        # ----------------------------------------------------
        # Discover new files
        # ----------------------------------------------------

        dataflow_jobs = []

        run_timestamp = datetime.now(
            timezone.utc
        ).strftime(
            "%Y%m%d%H%M%S"
        )

        for entity, config in ENTITY_CONFIG.items():

            prefix = f"incoming/{entity}/"

            print(
                f"Scanning: "
                f"gs://{DATA_BUCKET}/{prefix}"
            )

            blobs = storage_client.list_blobs(
                DATA_BUCKET,
                prefix=prefix,
            )

            for blob in blobs:

                # Only CSV files
                if not blob.name.endswith(".csv"):
                    continue

                file_path = (
                    f"gs://{DATA_BUCKET}/{blob.name}"
                )

                # ------------------------------------------------
                # Skip successfully processed files
                # ------------------------------------------------

                if file_path in processed_files:

                    print(
                        f"SKIP already processed: "
                        f"{file_path}"
                    )

                    continue

                # ------------------------------------------------
                # Create unique Dataflow job name
                # ------------------------------------------------

                file_hash = hashlib.md5(
                    file_path.encode("utf-8")
                ).hexdigest()[:8]

                job_name = (
                    f"rp-{entity}-"
                    f"{run_timestamp}-"
                    f"{file_hash}"
                )

                # Dataflow job names must be safe
                job_name = re.sub(
                    r"[^a-z0-9-]",
                    "-",
                    job_name.lower(),
                )

                # ------------------------------------------------
                # Build Dataflow parameters
                # ------------------------------------------------

                dataflow_job = {
                    "job_name": job_name,

                    "parameters": {

                        "inputFilePattern": (
                            file_path
                        ),

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

                dataflow_jobs.append(
                    dataflow_job
                )

                print(
                    f"NEW FILE: {file_path}"
                )

                print(
                    f"DATAFLOW JOB: {job_name}"
                )

        # ----------------------------------------------------
        # Summary
        # ----------------------------------------------------

        print(
            "======================================"
        )

        print(
            f"New files discovered: "
            f"{len(dataflow_jobs)}"
        )

        print(
            "======================================"
        )

        for job in dataflow_jobs:

            print(
                f"Job: {job['job_name']}"
            )

            print(
                f"File: "
                f"{job['parameters']['inputFilePattern']}"
            )

        # IMPORTANT:
        # Return the list directly.
        # This allows Airflow dynamic task mapping.
        return dataflow_jobs


    # ========================================================
    # RUN FILE DISCOVERY
    # ========================================================

    new_files = check_new_files()


    # ========================================================
    # 2. RUN DATAFLOW DYNAMICALLY
    # ========================================================

    run_dataflow = (
        DataflowTemplatedJobStartOperator.partial(

            task_id="run_dataflow",

            project_id=PROJECT_ID,

            location=REGION,

            template=DATAFLOW_TEMPLATE,

            wait_until_finished=True,
        )

        .expand_kwargs(
            new_files
        )
    )


    # ========================================================
    # 3. WRITE INGESTION AUDIT
    # ========================================================

    @task
    def record_ingestion_success(
        file_info
    ):

        from google.cloud import storage
        from google.cloud import bigquery

        storage_client = storage.Client(
            project=PROJECT_ID
        )

        bq_client = bigquery.Client(
            project=PROJECT_ID
        )

        # ----------------------------------------------------
        # Extract information from Dataflow mapping
        # ----------------------------------------------------

        parameters = file_info[
            "parameters"
        ]

        file_path = parameters[
            "inputFilePattern"
        ]

        job_name = file_info[
            "job_name"
        ]

        # ----------------------------------------------------
        # Extract GCS object path
        # ----------------------------------------------------

        bucket_prefix = (
            f"gs://{DATA_BUCKET}/"
        )

        object_name = file_path[
            len(bucket_prefix):
        ]

        # ----------------------------------------------------
        # Read GCS metadata
        # ----------------------------------------------------

        blob = (
            storage_client
            .bucket(DATA_BUCKET)
            .get_blob(object_name)
        )

        file_name = object_name.split(
            "/"
        )[-1]

        file_size = None
        file_generation = None

        if blob:

            file_size = blob.size

            file_generation = str(
                blob.generation
            )

        # ----------------------------------------------------
        # Identify entity
        # ----------------------------------------------------

        entity_name = "unknown"

        for entity in ENTITY_CONFIG:

            if (
                f"incoming/{entity}/"
                in object_name
            ):

                entity_name = entity

                break

        # ----------------------------------------------------
        # Timestamps
        # ----------------------------------------------------

        started_at = datetime.now(
            timezone.utc
        ).isoformat()

        completed_at = datetime.now(
            timezone.utc
        ).isoformat()

        created_at = datetime.now(
            timezone.utc
        ).isoformat()

        # ----------------------------------------------------
        # Build audit record
        # ----------------------------------------------------

        row = {

            "audit_id": str(
                uuid.uuid4()
            ),

            "file_name": file_name,

            "file_path": file_path,

            "entity_name": entity_name,

            "batch_id": job_name,

            "file_size_bytes": file_size,

            "file_generation": (
                file_generation
            ),

            "started_at": started_at,

            "completed_at": completed_at,

            "status": "SUCCESS",

            "row_count": None,

            "bad_row_count": None,

            "dataflow_job_name": job_name,

            "error_message": None,

            "created_at": created_at,
        }

        # ----------------------------------------------------
        # Insert audit record
        # ----------------------------------------------------

        errors = bq_client.insert_rows_json(
            AUDIT_TABLE,
            [row],

            row_ids=[
                row["audit_id"]
            ],
        )

        if errors:

            raise RuntimeError(
                "Failed to write ingestion "
                f"audit: {errors}"
            )

        print(
            "======================================"
        )

        print(
            "INGESTION SUCCESS"
        )

        print(
            f"File: {file_path}"
        )

        print(
            f"Dataflow Job: {job_name}"
        )

        print(
            f"Entity: {entity_name}"
        )

        print(
            "======================================"
        )


    # ========================================================
    # CREATE DYNAMIC AUDIT TASKS
    # ========================================================

    record_success = (
        record_ingestion_success
        .expand(
            file_info=new_files
        )
    )


    # ========================================================
    # DEPENDENCY
    # ========================================================

    run_dataflow >> record_success
