import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.baseoperator import chain

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryDeleteTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_files = list(map(
    lambda month: f"yellow_tripdata_2022-0{month}.parquet", [1, 2, 3, 4, 5, 6, 7, 8, 9]))
dataset_url_prefix = "https://d37ci6vzurychx.cloudfront.net/trip-data"
dataset_urls = dict(
    map(lambda file: (file, f"{dataset_url_prefix}/{file}"), dataset_files))

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

zones_file = "zones.csv"
zones_url = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
zones_parquet_file = zones_file.replace('.csv', '.parquet')

BIGQUERY_DATASET = os.environ.get("GCP_BIGQUERY_DATASET", 'trips_data_all')


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error(
            "Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['data-eng-class'],
) as dag:

    start_task = DummyOperator(
        task_id="start_task"
    )

    end_task = DummyOperator(
        task_id="end_task"
    )

    start_dataset_task = DummyOperator(
        task_id="start_dataset_task"
    )

    end_dataset_task = DummyOperator(
        task_id="end_dataset_task"
    )

    start_zones_task = DummyOperator(
        task_id="start_zones_task"
    )

    end_zones_task = DummyOperator(
        task_id="end_zones_task"
    )

    def download_task(file, url):
        task = BashOperator(
            task_id=f"download_{file}_task",
            bash_command=f"curl -sSL {url} > {path_to_local_home}/{file}"
        )
        return task

    def format_to_parquet_task(file):
        task = PythonOperator(
            task_id=f"format_to_parquet_task_{file}",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": f"{path_to_local_home}/{file}",
            },
        )
        return task

    def file_to_gcs_task(file):
        task = PythonOperator(
            task_id=f"{file}_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"raw/{file}",
                "local_file": f"{path_to_local_home}/{file}",
            },
        )
        return task

    def bigquery_delete_table(table_name):
        task = BigQueryDeleteTableOperator(
            task_id=f"bigquery_delete_{table_name}_task",
            ignore_if_missing=True,
            deletion_dataset_table=f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"
        )
        return task

    def bigquery_create_table_task(files, table_name):
        task = BigQueryCreateExternalTableOperator(
            task_id=f"bigquery_create_{table_name}_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": table_name,
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": list(map(lambda file: f"gs://{BUCKET}/raw/{file}", files))

                },
            },
        )
        return task

    chain(start_dataset_task,
          [download_task(file, url) for file, url in dataset_urls.items()],
          [file_to_gcs_task(file) for file in dataset_files],
          bigquery_delete_table('taxi_rides'), bigquery_create_table_task(dataset_files, 'taxi_rides'), end_dataset_task)

    chain(start_zones_task, download_task(zones_file, zones_url), format_to_parquet_task(zones_file),
          file_to_gcs_task(zones_parquet_file), bigquery_delete_table('zones'),
          bigquery_create_table_task([zones_parquet_file], 'zones'), end_zones_task)

    start_task >> [start_dataset_task, start_zones_task]

    end_task << [end_dataset_task, end_zones_task]
