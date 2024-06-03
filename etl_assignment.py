from datetime import datetime, timedelta, date
import requests
import json
from airflow import DAG
from airflow.models import XCom
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import mysql.connector
from pandasql import sqldf
import pandas as pd
import gspread
from google.oauth2 import service_account
import google.auth


from extract import extract_data
from dump_to_db import dump_to_database
from transform import transform_data
from load import load_data

default_args = {
    'owner': 'muzammil',
    'start_date': datetime(2023, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id="etl_assignment",
    default_args = default_args,
    schedule_interval=None
)

extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
        dag=dag
    )

dump_to_db = PythonOperator(
    task_id='dump_to_database',
    python_callable=dump_to_database,
    dag=dag
)

transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        dag=dag
    )

load = PythonOperator(
        task_id='load',
        python_callable=load_data,
        dag=dag
    )


extract >> dump_to_db >> transform >> load