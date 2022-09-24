from fileinput import filename
import os

from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET=os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME=os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET=os.environ.get("BIGQUERY_DATASET", "all_years_crashes_data")

file_data = ['accident', 'person', 'vehicle']

default_args={
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="GCS_to_BQ_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags = ['crashes']
) as dag:

    for file_name in file_data:
        bigquery_accident_exeternal_table_task = BigQueryCreateExternalTableOperator(
            task_id=f'bigquery_{file_name}_exeternal_table_task',
            table_resource={
                "tableReference":{
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f'{file_name}_external_table'
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/raw/{file_name}/*"]
                }
            }
        )