This Project demonstrates data engineering through Airflow and BigQuery.  There are 2 DAGs in this project.
1. Vehicle Data 
2. Weather API

##  Cluster Info.

Airflow URL: http://35.219.142.46:8080/ ( Credentials sent through email or reach out to me). Please note it's HTTP and not https
Data wharehouse: [BigQuery Publicly accessible wharehouse](https://console.cloud.google.com/bigquery?authuser=0&project=rk-airflow&ws=!1m4!1m3!3m2!1srk-airflow!2sworksample) ( you need a gmail account)
Object Store Bucket: [GCS Bucket](https://console.cloud.google.com/storage/browser/rk-logistic-bucket)

##  Vehicle Data DAG:

This assumes the source system data is located in Object store such as GCS.  Following are the design considerations that went into this DAG.

1. Object store operations are atomic. Files that are being uploaded is not visible to other users until it's done.
2. We don't delete all the files in GCS. Because a DAG is multi step/node, so it could be that a file is uploaded after we loaded the data into BigQuery, and we don't want to delete the file that's not loaded into BigQuery.


This DAG does following.
1. List the current GCS Objects.
2. Load this list of gcs files into vehicle historic table
3. Create latest table by partition on vehicle ID and only take latest value.
4. Delete the GCS objects only listed in # 1. We don't want to delete all the files because between 1 & 4, source system could upload more.


## Weather API DAG :

Weather APIs provide current weather, We want to use the power of DE to store histroic weather information. For the sake of this simplicity, we do this only for New York City.

DAG does the following:
1. Ingest the current weather info. As this data is small we put this into XCOM
2. We create the insert query dynamically using Jinja2 templating.
3. We invoke the query which will insert the data into weather table.


## Code Structure:
1. dags - This contains all the dag code.
2. sql -  This contains all the SQL and Jinja templated SQL. when deployed this directory should be located in sql
3. data - this is test data which you can copy into GCS Storage.
