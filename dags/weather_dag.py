import requests
from airflow import DAG 
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator

from airflow.utils.dates import days_ago

default_arguments = {'owner': 'Rakesh Koritala', 'start_date': days_ago(1)}
GCS_BUCKET_NAME = 'rk-logistic-bucket'
BQ_TABLE_NAME = 'rk-airflow.weather_historic'
GCP_CONNECTION_ID='google_cloud_default'
WEATHER_API_TOKEN = '9a536a673e5fdf908215876bcb6a8076'
with DAG(
    'weather_dag',
     schedule_interval='@hourly',
     catchup=False,
     default_args=default_arguments,
     render_template_as_native_obj=True
) as dag:

    def ingest_weather(**kwargs):
        parameters={'query':'New York City','access_key': WEATHER_API_TOKEN}
        response=requests.get('http://api.weatherstack.com/current', parameters)
        response.raise_for_status()
        result_str = response.json()
        return result_str['current']

    ingest_weather = PythonOperator(task_id='ingest_weather', python_callable=ingest_weather, provide_context=True)

    insert_query = BigQueryInsertJobOperator(
            task_id="insert_query_job",
            configuration={
                "query": {
                    "query": 'sql/weather_insert.sql',
                    "useLegacySql": False,
                }
            },
            location='us-west2'
    )


ingest_weather >> insert_query 
