from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
import random
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'Aemon',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 12),
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'daily_fuel_price_generation',
    default_args=default_args,
    description='Daily generation of fuel prices for gas stations',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False
)

# Wait for data collection DAGs
wait_for_bp_data_collection = ExternalTaskSensor(
    task_id='wait_for_bp_data_collection',
    external_dag_id='collection_data_from_bp_v1',
    external_task_id='check_and_insert_data',
    timeout=600,
    poke_interval=30,
    mode='poke',
    dag=dag,
)

wait_for_mobil_data_collection = ExternalTaskSensor(
    task_id='wait_for_mobil_data_collection',
    external_dag_id='collection_data_from_mobil_v5',
    external_task_id='check_and_insert_data',
    timeout=600,
    poke_interval=30,
    mode='poke',
    dag=dag,
)

wait_for_paknsave_data_collection = ExternalTaskSensor(
    task_id='wait_for_paknsave_data_collection',
    external_dag_id='collection_data_from_paknsave_v10',
    external_task_id='fetch_and_insert_gas_stations',
    timeout=600,
    poke_interval=30,
    mode='poke',
    dag=dag,
)

wait_for_z_data_collection = ExternalTaskSensor(
    task_id='wait_for_z_data_collection',
    external_dag_id='collection_data_from_z_v3',
    external_task_id='check_and_insert_data',
    timeout=600,
    poke_interval=30,
    mode='poke',
    dag=dag,
)

def generate_random_price(base_price):
    return round(random.uniform(base_price - 0.37, base_price + 0.23), 2)

def fetch_gaspy_prices():
    # 假设以下是新西兰最近一年的平均油价
    prices = {
        'Unleaded 91': 2.79,
        'Unleaded 95': 2.98,
        'Unleaded 98': 3.11,
        'Diesel': 2.10
    }
    return prices

def create_fuel_price_table():
    query = """
    CREATE TABLE IF NOT EXISTS fuel_price (
        id SERIAL PRIMARY KEY,
        location_id VARCHAR(50) REFERENCES gas_station(location_id),
        fuel_type VARCHAR(20),
        price DECIMAL(5, 2),
        date DATE DEFAULT CURRENT_DATE,
        UNIQUE (location_id, fuel_type, date)
    );
    """
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()
        logging.info("Table fuel_price created successfully")
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        raise

def insert_or_update_daily_fuel_prices():
    try:
        base_prices = fetch_gaspy_prices()
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute("SELECT location_id FROM gas_station")
        stations = cur.fetchall()

        for station in stations:
            location_id = station[0]
            for fuel_type, base_price in base_prices.items():
                price = generate_random_price(base_price)
                cur.execute("""
                    INSERT INTO fuel_price (location_id, fuel_type, price, date)
                    VALUES (%s, %s, %s, CURRENT_DATE)
                    ON CONFLICT (location_id, fuel_type, date)
                    DO UPDATE SET price = EXCLUDED.price
                """, (location_id, fuel_type, price))

        conn.commit()
        cur.close()
        conn.close()
        logging.info("Daily fuel prices inserted or updated successfully")
    except Exception as e:
        logging.error(f"Error inserting or updating fuel prices: {e}")
        raise

# Define the tasks
create_fuel_price_table_task = PythonOperator(
    task_id='create_fuel_price_table',
    python_callable=create_fuel_price_table,
    dag=dag,
)

insert_or_update_fuel_prices_task = PythonOperator(
    task_id='insert_or_update_fuel_prices',
    python_callable=insert_or_update_daily_fuel_prices,
    dag=dag,
)

# Set task dependencies
wait_for_bp_data_collection >> create_fuel_price_table_task
wait_for_mobil_data_collection >> create_fuel_price_table_task
wait_for_paknsave_data_collection >> create_fuel_price_table_task
wait_for_z_data_collection >> create_fuel_price_table_task
create_fuel_price_table_task >> insert_or_update_fuel_prices_task
