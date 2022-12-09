from airflow import DAG 
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.gcs  import  GCSListObjectsOperator, GCSDeleteObjectsOperator
from airflow.operators.python import PythonOperator

from airflow.utils.dates import days_ago

default_arguments = {'owner': 'Rakesh Koritala', 'start_date': days_ago(1)}
GCS_BUCKET_NAME = 'rk-logistic-bucket'
BQ_TABLE_NAME = 'rk-airflow.worksample.vehicles_latest'
GCP_CONNECTION_ID='google_cloud_default'
with DAG(
    'vehicles_data_dag',
     schedule_interval='@hourly',
     catchup=False,
     default_args=default_arguments,
     render_template_as_native_obj=True
) as dag:

    def __gcs_delete(**kwargs):
        ti = kwargs["ti"]
        list_gcs_files =  GCSDeleteObjectsOperator(
            task_id='delete_gcs_files',
            bucket_name=GCS_BUCKET_NAME,
            objects=ti.xcom_pull(task_ids="list_gcs_files"),
            gcp_conn_id=GCP_CONNECTION_ID
        )
        list_gcs_files.execute(kwargs)

    def __bigquery_import(**kwargs):
        ti = kwargs["ti"]
        source_objects = ti.xcom_pull(task_ids="list_gcs_files")
        if source_objects:
            load_data= GoogleCloudStorageToBigQueryOperator(
                task_id='load_data',
                bucket=GCS_BUCKET_NAME,
                source_objects=source_objects,
                source_format='CSV',
                skip_leading_rows=1,
                field_delimiter=',',
                destination_project_dataset_table='rk-airflow.worksample.vehicles_history',
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_APPEND',
                gcp_conn_id=GCP_CONNECTION_ID
            )
            load_data.execute(kwargs)

    list_gcs_files = GCSListObjectsOperator(
        task_id='list_gcs_files',
        bucket=GCS_BUCKET_NAME,
        prefix='vehicles_',
        delimiter='.csv',
        gcp_conn_id=GCP_CONNECTION_ID
    )
    load_data = PythonOperator(task_id='load_data', python_callable=__bigquery_import, provide_context=True)
    create_table = BigQueryOperator(
            task_id='create_table',
            sql='sql/vehicle_latest.sql',
            destination_dataset_table=BQ_TABLE_NAME,
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED',
            use_legacy_sql=False,
            location='us-west2',
            gcp_conn_id=GCP_CONNECTION_ID
    )
    delete_gcs_files = PythonOperator(task_id='delete_data', python_callable=__gcs_delete, provide_context=True)

list_gcs_files >> load_data >> create_table >> delete_gcs_files
