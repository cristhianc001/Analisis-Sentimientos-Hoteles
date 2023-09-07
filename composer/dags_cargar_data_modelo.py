from airflow import DAG 
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

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

with DAG('Cargar_Data_Modelo', schedule_interval=None, default_args=default_args) as dag:

    inicio = DummyOperator(
        task_id = 'inicio',
        dag = dag
        )

    cargar_data = DummyOperator(
        task_id = 'cargar_data',
        trigger_rule=TriggerRule.ALL_DONE,
        dag = dag
        )    
    
    cargar_data_reviews_model = GCSToBigQueryOperator(
        task_id = 'cargar_data_reviews_model',
        bucket = BUCKET_NAME,
        source_objects = ['reviews-model.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{DATASET}.reviews_model',
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
            {'name': 'overall_sentiment', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'room_sentiment', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'guest_service_sentiment', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'cleaning_sentiment', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'breakfast_sentiment', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'sentiment_score', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'clean_review', 'type': 'STRING', 'mode': 'REQUIRED'},
        ],
        trigger_rule=TriggerRule.ALL_DONE,
        dag = dag
        )

    fin = DummyOperator(
        task_id = 'fin',
        trigger_rule=TriggerRule.ALL_DONE,
        dag = dag
        ) 
    
inicio >> cargar_data >> cargar_data_reviews_model >> fin