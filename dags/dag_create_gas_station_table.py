from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging

default_args = {
    'owner': 'Aemon',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 12),
    'retries': 5,
}

dag = DAG(
    'create_gas_station_table',
    default_args=default_args,
    description='Create gas_station table in PostgreSQL',
    schedule_interval='@once',  # Run only once
    catchup=False
)

create_table_sql = """
CREATE TABLE IF NOT EXISTS gas_station (
    location_id VARCHAR(50) PRIMARY KEY,
    brand_name VARCHAR(255),
    location_name VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    address_line1 VARCHAR(255),
    city VARCHAR(255),
    state_province VARCHAR(255),
    postal_code VARCHAR(50),
    country VARCHAR(50)
)
"""

def log_success():
    logging.info("Successfully created the gas_station table.")

create_table_task = PostgresOperator(
    task_id='Team_create_gas_station_table',
    postgres_conn_id='postgres_default',
    sql=create_table_sql,
    dag=dag,
)

log_task = PythonOperator(
    task_id='log_success',
    python_callable=log_success,
    dag=dag,
)

create_table_task >> log_task
