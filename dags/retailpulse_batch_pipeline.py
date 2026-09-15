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

# Dataflow regional endpoint
REGION = "us-central1"

DATA_BUCKET = "ecommerce-data-bt"

AUDIT_TABLE = f"{PROJECT_ID}.ecommerce_bronze.ingestion_audit"

DATAFLOW_TEMPLATE = (
    f"gs://dataflow-templates-{REGION}/latest/GCS_CSV_to_BigQuery"
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
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["retailpulse", "batch", "gcp"],
) as dag:

    # ========================================================
    # TASK 1: CHECK FOR NEW FILES
    # ========================================================

    @task
    def check_new_files():
        from google.cloud import storage
        from google.cloud import bigquery

        storage_client = storage.Client(project=PROJECT_ID)
        bq_client = bigquery.Client(project=PROJECT_ID)

        # ----------------------------------------------------
        # Find files that were already successfully processed
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

        print(f"Already processed files: {len(processed_files)}")

        # ----------------------------------------------------
        # Prepare Dataflow jobs
        # ----------------------------------------------------

        dataflow_jobs = []

        run_timestamp = datetime.now(timezone.utc).strftime(
            "%Y%m%d%H%M%S"
        )

        # ----------------------------------------------------
        # Scan each entity folder
        # ----------------------------------------------------

        for entity, config in ENTITY_CONFIG.items():

            prefix = f"incoming/{entity}/"

            print(
                f"Scanning: gs://{DATA_BUCKET}/{prefix}"
            )

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

                # ------------------------------------------------
                # Duplicate prevention
                # ------------------------------------------------

                if file_path in processed_files:

                    print(
                        f"SKIP already processed: {file_path}"
                    )

                    continue

                # ------------------------------------------------
                # Generate deterministic job suffix
                # ------------------------------------------------

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

                # ------------------------------------------------
                # Dataflow job configuration
                # ------------------------------------------------

                dataflow_jobs.append(
                    {
                        "job_name": job_name,

                        "parameters": {

                            # Source CSV
                            "inputFilePattern": file_path,

                            # BigQuery schema
                            "schemaJSONPath": (
                                f"gs://{DATA_BUCKET}/schema/"
                                f"{config['schema']}"
                            ),

                            # BigQuery Bronze table
                            "outputTable": (
                                f"{PROJECT_ID}:"
                                f"ecommerce_bronze."
                                f"{config['table']}"
                            ),

                            # Temporary location
                            "bigQueryLoadingTemporaryDirectory": (
                                f"gs://{DATA_BUCKET}/temp/"
                            ),

                            # Bad records table
                            "badRecordsOutputTable": (
                                f"{PROJECT_ID}:"
                                f"ecommerce_bronze."
                                f"{config['bad_table']}"
                            ),

                            # CSV configuration
                            "delimiter": ",",
                            "csvFormat": "Default",
                            "containsHeaders": "true",

                            # ------------------------------------------------
                            # IMPORTANT:
                            # Explicit Dataflow worker zone
                            # ------------------------------------------------
                            "workerZone": "us-central1-b",
                        },
                    }
                )

                print(
                    f"NEW FILE: {file_path}"
                )

                print(
                    f"DATAFLOW JOB: {job_name}"
                )

        # ====================================================
        # SUMMARY
        # ====================================================

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

        return dataflow_jobs


    # ========================================================
    # TASK 2: DISCOVER NEW FILES
    # ========================================================

    new_files = check_new_files()


    # ========================================================
    # TASK 3: RUN DATAFLOW
    # ========================================================

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


    # ========================================================
    # TASK 4: RECORD SUCCESS IN INGESTION AUDIT
    # ========================================================

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

        # ----------------------------------------------------
        # Extract Dataflow information
        # ----------------------------------------------------

        parameters = file_info["parameters"]

        file_path = parameters[
            "inputFilePattern"
        ]

        job_name = file_info[
            "job_name"
        ]

        # ----------------------------------------------------
        # Get GCS object information
        # ----------------------------------------------------

        bucket_prefix = (
            f"gs://{DATA_BUCKET}/"
        )

        object_name = file_path[
            len(bucket_prefix):
        ]

        blob = (
            storage_client
            .bucket(DATA_BUCKET)
            .get_blob(object_name)
        )

        file_name = (
            object_name.split("/")[-1]
        )

        file_size = None
        file_generation = None

        if blob:

            file_size = blob.size

            file_generation = str(
                blob.generation
            )

        # ----------------------------------------------------
        # Determine entity
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
        # Audit timestamps
        # ----------------------------------------------------

        started_at = (
            datetime.now(timezone.utc)
            .isoformat()
        )

        completed_at = (
            datetime.now(timezone.utc)
            .isoformat()
        )

        created_at = (
            datetime.now(timezone.utc)
            .isoformat()
        )

        # ----------------------------------------------------
        # Create audit record
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

            "file_generation": file_generation,

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
                f"Failed to write ingestion audit: "
                f"{errors}"
            )

        # ----------------------------------------------------
        # Logging
        # ----------------------------------------------------

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
    # TASK 5: RECORD SUCCESS FOR EACH FILE
    # ========================================================

    record_success = (
        record_ingestion_success
        .expand(
            file_info=new_files
        )
    )


    # ========================================================
    # DEPENDENCIES
    # ========================================================

    run_dataflow >> record_success
