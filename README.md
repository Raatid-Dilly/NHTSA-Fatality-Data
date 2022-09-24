# NHTSA-Fatality-Data
National Highway Traffic Safety Administration Data on Fatalities from 1975-2020

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

The official [NHTSA Manual](https://github.com/Raatid-Dilly/NHTSA-Fatality-Data/blob/main/Fatality%20Analysis%20Reporting%20System%20(FARS)%20Analytical%20User’s%20Manual%2C%201975-2020.pdf) which contains a description of all columns and information relating to the values used is also included as a pdf file labeled Fatality Analysis Reporting System (FARS) Analytical User’s Manual, 1975-2020.pdf

## Technologies Used
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

## Architecture

