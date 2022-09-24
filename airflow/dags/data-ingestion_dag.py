import os
import zipfile
import chardet
import pyarrow.csv as pv
import pyarrow.parquet as pq
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "<YOUR BQ DATASET>")

url_template = 'https://static.nhtsa.gov/nhtsa/downloads/FARS/'
year_template = '{{ execution_date.strftime(\'%Y\') }}/National/FARS{{ execution_date.strftime(\'%Y\') }}NationalCSV.zip'
year = '{{ execution_date.strftime(\'%Y\') }}'

def unzip_and_format_to_parquet(src_file: str):
    """Convert csv file to parquet

    Args: 
        src_file: Source csv file
    """

    files_to_extract = ['accident.csv', 'person.csv', 'vehicle.csv']
    year_of_file = ''.join(char for char in src_file if char.isdigit())

    with zipfile.ZipFile(src_file, 'r') as f:
        for file_name in f.namelist():
            if file_name.lower() in files_to_extract:
                f.extract(file_name)
                table=pv.read_csv(file_name)
                pq.write_table(table, f"{AIRFLOW_HOME}/{year_of_file}_{file_name.lower().replace('.csv', '.parquet')}")
            else:
                continue

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


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
}

with DAG(
    dag_id='car_crash_data_ingestion',
    schedule_interval='@yearly',
    start_date=datetime(1975, 1, 1),
    end_date=datetime(2020, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['crash-analysis']
) as dag:

    download_car_crash_dataset_task = BashOperator(
        task_id = "download_car_crash_dataset_task",
        bash_command=f"wget -q -O {AIRFLOW_HOME}/{year}_car.zip {url_template + year_template}"
    )

    unzip_and_format_to_parquet_task = PythonOperator(
        task_id="unzip_and_format_to_parquet_task",
        python_callable=unzip_and_format_to_parquet,
        op_kwargs={
            "src_file": f"{AIRFLOW_HOME}/{year}_car.zip"
        }
    )

    upload_accident_parquet_to_GCS_task=PythonOperator(
        task_id='upload_accident_parquet_to_GCS_task',
        python_callable=upload_to_gcs_bucket,
        op_kwargs={
            'bucket_name': BUCKET,
            'source_file_name': f"{AIRFLOW_HOME}/{year}_accident.parquet",
            'destination_blob_name': f"raw/accident/{year}_accident.parquet"
        }
    )

    upload_person_parquet_to_GCS_task=PythonOperator(
        task_id='upload_person_parquet_to_GCS_task',
        python_callable=upload_to_gcs_bucket,
        op_kwargs={
            'bucket_name': BUCKET,
            'source_file_name': f"{AIRFLOW_HOME}/{year}_person.parquet",
            'destination_blob_name': f"raw/person/{year}_person.parquet"
        }
    )

    upload_vehicle_parquet_to_GCS_task=PythonOperator(
        task_id='upload_vehicle_parquet_to_GCS_task',
        python_callable=upload_to_gcs_bucket,
        op_kwargs={
            'bucket_name': BUCKET,
            'source_file_name': f"{AIRFLOW_HOME}/{year}_vehicle.parquet",
            'destination_blob_name': f"raw/vehicle/{year}_vehicle.parquet"
        }
    )


    rm_task=BashOperator(
        task_id="rm_task",
        bash_command=f"cd {AIRFLOW_HOME} && rm *.parquet"
    )

    download_car_crash_dataset_task >> unzip_and_format_to_parquet_task >> upload_accident_parquet_to_GCS_task >> upload_person_parquet_to_GCS_task >> upload_vehicle_parquet_to_GCS_task >> rm_task 



