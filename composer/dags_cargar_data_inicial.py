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

def execute_function_data_initial():
    url = 'https://us-central1-subtle-seat-394914.cloudfunctions.net/data-initial'
    try:
        response = requests.post(url, timeout=370)
        return True
    except requests.exceptions.RequestException as err:
        #Excepción generada por la solicitud
        return False

with DAG('Cargar_Data_Inicial', schedule_interval=None, default_args=default_args) as dag:

    inicio = DummyOperator(
        task_id = 'inicio',
        dag = dag
        )

    ejecutar_cloud_function_data_initial = PythonOperator(
        task_id='ejecutar_cloud_function_data_initial',
        python_callable=execute_function_data_initial,
        trigger_rule=TriggerRule.ALL_DONE,
        dag = dag
    )

    validar_cloud_function_data_initial = ShortCircuitOperator(
        task_id='validar_cloud_function_data_initial',
        python_callable=execute_function_data_initial,
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag
    )

    cargar_data = DummyOperator(
        task_id = 'cargar_data',
        trigger_rule=TriggerRule.ALL_DONE,
        dag = dag
        )    
    
    cargar_data_dim_guests = GCSToBigQueryOperator(
        task_id = 'cargar_data_dim_guests',
        bucket = BUCKET_NAME,
        source_objects = ['guests.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{DATASET}.guests',
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

    cargar_data_dim_lodgings = GCSToBigQueryOperator(
        task_id = 'cargar_data_dim_lodgings',
        bucket = BUCKET_NAME,
        source_objects = ['lodgings.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{DATASET}.lodgings',
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

    cargar_data_fact_reviews = GCSToBigQueryOperator(
        task_id = 'cargar_data_fact_reviews',
        bucket = BUCKET_NAME,
        source_objects = ['reviews.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{DATASET}.reviews',
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
    
    cargar_data_dim_calendar = GCSToBigQueryOperator(
        task_id = 'cargar_data_dim_calendar',
        bucket = BUCKET_NAME,
        source_objects = ['calendar.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{DATASET}.calendar',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
            {'name': 'date', 'type': 'DATE', 'mode': 'REQUIRED'},
            {'name': 'year', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'quarter', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'month', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        ],
        trigger_rule=TriggerRule.ALL_DONE,
        dag = dag
        )   

    fin = DummyOperator(
        task_id = 'fin',
        trigger_rule=TriggerRule.ALL_DONE,
        dag = dag
        ) 
    
inicio >> ejecutar_cloud_function_data_initial >> validar_cloud_function_data_initial >> cargar_data >> cargar_data_dim_guests >> cargar_data_dim_lodgings >> cargar_data_fact_reviews >> cargar_data_dim_calendar >> fin