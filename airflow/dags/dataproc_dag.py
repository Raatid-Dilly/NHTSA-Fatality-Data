import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator, DataprocDeleteClusterOperator, DataprocCreateClusterOperator
from google.cloud import storage

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "<YOUR BQ DATASET>")
REGION = os.environ.get("GCP_REGION")
DATAPROC_CLUSTER = os.environ.get("GCP_DATAPROC_CLUSTER", "<YOUR BQ CLUSTER NAME>")
pyspark_file = 'pyspark_data_transform.py'

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": "1"
}

def upload_to_gcs_bucket(bucket_name, source_file_name, destination_blob_name):
    """Upload a file to google storage bucket
    
    Args:
        bucket_name: Name of google bucket
        source_file_name: Path of local file to be uploaded
        destination_blob_name: ID of GCS object "storage-object-name"
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

with DAG(
    dag_id="dataproc_spark_job",
    schedule_interval="@once",
    catchup=True,
    max_active_runs=1,
    tags=['crash-analysis'],
    default_args=default_args
) as dag:

    create_dataproc_cluster_task=DataprocCreateClusterOperator(
        task_id = "create_dataproc_cluster_task",
        project_id = PROJECT_ID,
        cluster_name = DATAPROC_CLUSTER,
        num_workers = 2,
        worker_machine_type = "n1-standard-4",
        region = REGION
    )

    upload_spark_file_to_gcs_task = PythonOperator(
        task_id = "upload_spark_file_to_gcs_task",
        python_callable= upload_to_gcs_bucket,
        op_kwargs={
            "bucket_name": BUCKET,
            "source_file_name": AIRFLOW_HOME + '/' + pyspark_file,
            "destination_blob_name": "code/" + pyspark_file
        }
    )

    submit_dataproc_pyspark_task = DataprocSubmitPySparkJobOperator(
        task_id = "submit_dataproc_pyspark_task",
        main = f"gs://{BUCKET}/code/{pyspark_file}",
        arguments = [f"--input_accident=gs://{BUCKET}/raw/accident", f"--input_person=gs://{BUCKET}/raw/person", f"--input_vehicle=gs://{BUCKET}/raw/vehicle"],
        region = REGION,
        cluster_name = DATAPROC_CLUSTER,
        dataproc_jars = ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.24.0.jar"]
    )

    delete_dataproc_task = DataprocDeleteClusterOperator(
        task_id = "delete_dataproc_task",
        project_id = PROJECT_ID,
        cluster_name = DATAPROC_CLUSTER,
        region = REGION,
        trigger_rule = "all_done"
    )

    create_dataproc_cluster_task >> upload_spark_file_to_gcs_task >> submit_dataproc_pyspark_task >> delete_dataproc_task