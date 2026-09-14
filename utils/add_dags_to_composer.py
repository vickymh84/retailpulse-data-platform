from __future__ import annotations

import argparse
import glob
import os
from shutil import copytree, ignore_patterns
import tempfile

from google.cloud import storage


def _create_dags_list(dags_directory: str) -> tuple[str, list[str]]:
    temp_dir = tempfile.mkdtemp()

    files_to_ignore = ignore_patterns(
        "__init__.py",
        "*_test.py"
    )

    copytree(
        dags_directory,
        f"{temp_dir}/",
        ignore=files_to_ignore,
        dirs_exist_ok=True,
    )

    dags = glob.glob(f"{temp_dir}/*.py")

    return (temp_dir, dags)


def upload_dags_to_composer(
    dags_directory: str,
    bucket_name: str,
    name_replacement: str = "dags/",
) -> None:

    temp_dir, dags = _create_dags_list(dags_directory)

    if len(dags) > 0:

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        for dag in dags:

            dag = dag.replace(
                f"{temp_dir}/",
                name_replacement,
            )

            try:

                blob = bucket.blob(dag)

                blob.upload_from_filename(dag)

                print(
                    f"File {dag} uploaded to "
                    f"{bucket_name}/{dag}."
                )

            except FileNotFoundError:

                current_directory = os.listdir()

                print(
                    f"{name_replacement} directory not found "
                    f"in {current_directory}, "
                    f"you may need to override the default "
                    f"value of name_replacement."
                )

                raise

    else:
        print("No DAGs to upload.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--dags_directory",
        help="Relative path to the source directory containing your DAGs",
    )

    parser.add_argument(
        "--dags_bucket",
        help="Name of the DAGs bucket of your Composer environment without the gs:// prefix",
    )

    args = parser.parse_args()

    upload_dags_to_composer(
        args.dags_directory,
        args.dags_bucket,
    )
