# NHTSA-Fatality-Data
National Highway Traffic Safety Administration Fatality Analysis Report for data from 1975-2020.

**Google DataStudio Dashboard for results can be viewed [here](https://datastudio.google.com/reporting/39c186d2-90ba-4d1a-8d1a-2db046e93641).**

## Project Overview
This project was performed as part of the [Data Engineering Zoomcamp Course](https://github.com/DataTalksClub/data-engineering-zoomcamp) to use the course materials learned to build a data pipeline.

## Objective
The National Highway Traffic Safety Administration is an agency that is part of the United States federal government. Their mission is the preservation of human lives and to reduce vehicle-related accidents related to transportation.

The goals of this project are the following:
* Develop a data pipeline that will extract the data from the source for each year from 1975-2000
* Transform the data so it can be analyzed
* Load the final data into a BigQuery table
* Build an analytic dashboard to view trends on the data. Such as states with the most fatalities due to car accidents and the weather conditions in which most accidents occur.

## NHTSA Dataset
The dataset used can be found here on the [NHTSA](https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/) site where data for each year is stored in a separate folder. In the folder for each year, there is a subdirectory named 'National' that contains the .zip file with all the data. In each .zip file, the following .csv files can be found:
* Accident.csv
  - Contains information such as the time of the accident, number of persons involved, weather conditions, light conditions, etc.
* Person.csv
  - Contains information such as the gender of the driver, age, race, etc.
* Vehicle.csv
  - Contains information such as the vehicle make, model, number of occupants, etc.

The official [NHTSA Manual](https://github.com/Raatid-Dilly/NHTSA-Fatality-Data/blob/main/Fatality%20Analysis%20Reporting%20System%20(FARS)%20Analytical%20User’s%20Manual%2C%201975-2020.pdf) which contains a description of all columns and information relating to the values used is also included as a .pdf file labeled Fatality Analysis Reporting System (FARS) Analytical User’s Manual, 1975-2020.pdf

## Architecture

![alt workflow](https://github.com/Raatid-Dilly/NHTSA-Fatality-Data/blob/main/images/NHTSA.jpg)
The project utilizes the following technologies:
* Cloud: 
  - Google Cloud Platform:
    - Google Cloud Storage for Data Lake
    - Google BigQuery for Data Warehouse
* Infrastructure as Code (Iac):
  - Terraform
* Workflow Container:
  - Docker
* Workflow Orchestration:
  - Apache Airflow
* Data Transformation:
  - Apache Spark (PySpark)
* Data Visualization
  - Google Data Studio

# Work
**Local Development** - Before beginning the cloud process, the first step I took was local analysis of the data. To begin locally run the [```download_data.sh```](https://github.com/Raatid-Dilly/NHTSA-Fatality-Data/blob/main/local/download_data.sh) script which will download all the ```['Accident.csv', Person.csv, Vehicle.csv]``` files from the NHTSA site. Next use the [```local_spark.py```](https://github.com/Raatid-Dilly/NHTSA-Fatality-Data/blob/main/local/local_spark.py) script to format the columns in each .csv file and save the .csv to .parquet. After this is complete the parquet files could then be read and transformed with PySpark.

```
df = spark.read.option('header', 'true').parquet('./local/accident.parquet')

#Removes the headers that were added in the UnionAll from the df
df = df.filter(df.ST_CASE != 'ST_CASE')

def states(state):
    
    dict_states = {
        '1': 'Alabama', '2': 'Alaska', '4': 'Arizona', '5': 'Arkansas', '6': 'California', '8': 'Colorado', 
        '9': 'Connecticut', '10': 'Delaware', '11': 'District of Columbia', '12': 'Florida','13': 'Georgia', 
        '15': 'Hawaii', '16': 'Idaho', '17': 'Illinois', '18': 'Indiana', '19': 'Iowa','20': 'Kansas', 
        '21': 'Kentucky', '22': 'Louisiana', '23': 'Maine', '24': 'Maryland', '25': 'Massachusetts', 
        '26': 'Michigan', '27': 'Minnesota', '28': 'Mississippi', '29': 'Missouri','30': 'Montana', 
        '31': 'Nebraska', '32': 'Nevada', '33': 'New Hamsphire', '34': 'New Jersey','35': 'New Mexico', 
        '36': 'New York', '37': 'North Carolina', '38': 'North Dakota', '39': 'Ohio', '40': 'Oklahoma', 
        '41': 'Oregon', '42': 'Pennsylvania', '43': 'Puerto Rico', '44': 'Rhode Island', '45': 'South Carolina', 
        '46': 'South Dakota', '47': 'Tennessee', '48': 'Texas','49': 'Utah', '50': 'Vermont', 
        '51': 'Virgin Islands', '52': 'Virginia', '53': 'Washington', '54': 'West Virginia', 
        '55': 'Wisconsin', '56': 'Wyoming'
    }

    if state in dict_states:
        return dict_states[state]
    else:
        return "Unknown"
    
states_udf = F.udf(states, returnType=types.StringType())
#State Column
df = df.withColumn('STATE', states_udf(df.STATE))

df.select('STATE').distinct().show()
```

**Google Cloud Platform** - Next was creating a [GCP account](https://cloud.google.com). This included creating a new project in the cloud for this project and setting the necessary permission that are required to have access to files. These include (Storage Admin, Storage Object Admin, BigQuery Admin, Dataproc Administrator, and Dataproc Worker). It is also important to download the google auth-keys .json file on the Service Account Page. Refer [here](https://cloud.google.com/docs/authentication/provide-credentials-adc) for instructions on how to properly authenticate.

**Infrastructure as Code Iac (Terraform)** - Terraform is used to setup the neccessary google cloud storage(Data Lake) and BigQuery Datasets (Data Warehouse) for the project. To begin the Terraform process, go to the Terraform directory in your CLI and run the following:
  - ```terraform init```
  - ```terraform plan``` (Will need to specify GCP Project)
  - ```terraform apply``` (Will need to specify GCP Project)
  - The ```terraform destroy``` command will remove the the storage and BigQuery Datasets from the Cloud
  
**Terraform configuaration files can be found [here](https://github.com/Raatid-Dilly/NHTSA-Fatality-Data/tree/main/terraform)**

**Apache Airflow Orchestration** - Apache Airflow is used to orchestrate the data ingestion and transformation pipelines and is ran in a [Docker](https://www.docker.com) container locally. The [airflow/dags](https://github.com/Raatid-Dilly/NHTSA-Fatality-Data/tree/main/airflow/dags) directory contains the data-ingestion and transformation scripts that were executed.
- ```data-ingestion_dag.py``` - DAG that downloads the NHTSA data for each year from 1975-2020 and uploads the information to the created Google data lake.
- ```create_external_table_dag.py``` - DAG to create an external table for viewing the uploaded data from the data lake
- ```dataproc_dag.py``` - DAG to create the DataProc Cluster and submit the PySpark job that performs the necessary data transformation. The script that will be used is found in the ```airflow``` directory and is [```pyspark_data_transform.py```](https://github.com/Raatid-Dilly/NHTSA-Fatality-Data/blob/main/airflow/pyspark_data_transform.py) After the job is finished and saved to a BigQuery Dataset, the Cluster is deleted as to not incur usage fees.

# Results
**Google DataStudio Dashboard can be viewed [here](https://datastudio.google.com/reporting/39c186d2-90ba-4d1a-8d1a-2db046e93641).**

**Dashboard Example** - Here is an example of the Dashboard filtered by Chevrolet vehicles. It is clear that most fatalies involving Chevrolets occur in Texas and the amount of accidents and the amount of deaths has been on a decline since 1975.

![alt dashboard](https://github.com/Raatid-Dilly/NHTSA-Fatality-Data/blob/main/images/NHTSA_Fatality_Analysis_Report_(FARS)1.jpg)
