import airflow
from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import ShortCircuitOperator
from datetime import datetime, timedelta
import requests

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID="subtle-seat-394914"
BUCKET_NAME = 'data-datawarehouse'
DATASET = "reviews_dw"

default_args = {
    'owner': 'Yaneth Ramirez',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'start_date': datetime(2023, 1, 1),
    'retry_delay': timedelta(minutes=5),
}

def execute_function_data_api_google():
    url = 'https://us-central1-subtle-seat-394914.cloudfunctions.net/data-api-google'
    try:
        response = requests.post(url, timeout=370)
        return True
    except requests.exceptions.RequestException as err:
        #Excepción generada por la solicitud
        return False
    
def execute_function_data_api_dw():
    url = 'https://us-central1-subtle-seat-394914.cloudfunctions.net/data-api-dw'
    try:
        response = requests.post(url, timeout=370)
        return True
    except requests.exceptions.RequestException as err:
        #Excepción generada por la solicitud
        return False
    
def execute_function_data_merge_dw():
    url = 'https://us-central1-subtle-seat-394914.cloudfunctions.net/data-merge-dw'
    try:
        response = requests.post(url, timeout=370)
        return True
    except requests.exceptions.RequestException as err:
        #Excepción generada por la solicitud
        return False

with DAG('Cargar_Data_API', schedule_interval=None, default_args=default_args) as dag:

    inicio = DummyOperator(
        task_id = 'inicio',
        dag = dag
        )

    ejecutar_cloud_function_data_api_google = PythonOperator(
        task_id='ejecutar_cloud_function_data_api_google',
        python_callable=execute_function_data_api_google,
        trigger_rule=TriggerRule.ALL_DONE,
        dag = dag
    )

    validar_cloud_function_data_api_google = ShortCircuitOperator(
        task_id='validar_cloud_function_data_api_google',
        python_callable=execute_function_data_api_google,
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag
    )

    ejecutar_cloud_function_data_api_dw = PythonOperator(
        task_id='ejecutar_cloud_function_data_api_dw',
        python_callable=execute_function_data_api_dw,
        trigger_rule=TriggerRule.ALL_DONE,
        dag = dag
    )

    validar_cloud_function_data_api_dw = ShortCircuitOperator(
        task_id='validar_cloud_function_data_api_dw',
        python_callable=execute_function_data_api_dw,
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag
    )

    cargar_data = DummyOperator(
            task_id = 'cargar_data',
            trigger_rule=TriggerRule.ALL_DONE,
            dag = dag
            )    
    
    cargar_data_guests_temp = GCSToBigQueryOperator(
        task_id = 'cargar_data_guests_temp',
        bucket = BUCKET_NAME,
        source_objects = ['guests-api.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{DATASET}.guests_temp',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
            {'name': 'guest_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'guest_code', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'guest_name', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'created_date', 'type': 'DATETIME', 'mode': 'NULLABLE'},
            {'name': 'updated_date', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        ],
        trigger_rule=TriggerRule.ALL_DONE,
        dag = dag
        )

    cargar_data_lodgings_temp = GCSToBigQueryOperator(
        task_id = 'cargar_data_lodgings_temp',
        bucket = BUCKET_NAME,
        source_objects = ['lodgings-api.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{DATASET}.lodgings_temp',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
            {'name': 'lodging_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'lodging_code', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'lodging_name', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'latitude', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'longitude', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'state', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'created_date', 'type': 'DATETIME', 'mode': 'NULLABLE'},
            {'name': 'updated_date', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        ],
        trigger_rule=TriggerRule.ALL_DONE,
        dag = dag
        )

    cargar_data_reviews_temp = GCSToBigQueryOperator(
        task_id = 'cargar_data_reviews_temp',
        bucket = BUCKET_NAME,
        source_objects = ['reviews-api.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{DATASET}.reviews_temp',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
            {'name': 'review_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'lodging_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'guest_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'date', 'type': 'DATE', 'mode': 'REQUIRED'},
            {'name': 'review', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'rating', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'created_date', 'type': 'DATETIME', 'mode': 'NULLABLE'},
            {'name': 'updated_date', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        ],
        trigger_rule=TriggerRule.ALL_DONE,
        dag = dag
        )

    ejecutar_cloud_function_data_merge_dw = PythonOperator(
        task_id='ejecutar_cloud_function_data_merge_dw',
        python_callable=execute_function_data_merge_dw,
        trigger_rule=TriggerRule.ALL_DONE,
        dag = dag
    )

    validar_cloud_function_data_merge_dw = ShortCircuitOperator(
        task_id='validar_cloud_function_data_merge_dw',
        python_callable=execute_function_data_merge_dw,
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag
    )

    fin = DummyOperator(
        task_id = 'fin',
        trigger_rule=TriggerRule.ALL_DONE,
        dag = dag
        ) 
    
inicio >> ejecutar_cloud_function_data_api_google >> validar_cloud_function_data_api_google >> ejecutar_cloud_function_data_api_dw >> validar_cloud_function_data_api_dw >> cargar_data >> cargar_data_guests_temp >> cargar_data_lodgings_temp >> cargar_data_reviews_temp >> ejecutar_cloud_function_data_merge_dw >> validar_cloud_function_data_merge_dw >> fin