from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import psycopg2

# Database connection parameters
db_params = {
    'database': 'chchfr',
    'user': 'postgres',
    'password': '8mhtjGJXz8KgisMJRWMC',
    'host': 'data472-hwa205-database-1.cyi9p9kw8doa.ap-southeast-2.rds.amazonaws.com',
    'port': '5432'
}

# Default arguments
default_args = {
    'owner': 'Aemon',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 12),
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'daily_fuel_price_v12',
    default_args=default_args,
    description='Daily generation of fuel prices for gas stations',
    schedule_interval='0 0 * * *',
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

def create_gas_station_table():
    query = """
    CREATE TABLE IF NOT EXISTS gas_station (
        location_id VARCHAR(50) PRIMARY KEY,
        brand_name VARCHAR(50),
        location_name VARCHAR(50),
        latitude FLOAT,
        longitude FLOAT,
        address_line1 VARCHAR(100),
        city VARCHAR(50),
        state_province VARCHAR(50),
        postal_code VARCHAR(20),
        country VARCHAR(50)
    );
    """
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()
        print("Table gas_station created successfully")
    except Exception as e:
        print("Error:", e)

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
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()
        print("Table fuel_price created successfully")
    except Exception as e:
        print("Error:", e)

create_gas_station_table_task = PythonOperator(
    task_id='create_gas_station_table',
    python_callable=create_gas_station_table,
    dag=dag,
)

create_fuel_price_table_task = PythonOperator(
    task_id='create_fuel_price_table',
    python_callable=create_fuel_price_table,
    dag=dag,
)

def insert_or_update_daily_fuel_prices():
    try:
        base_prices = fetch_gaspy_prices()
        conn = psycopg2.connect(**db_params)
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
        print("Daily fuel prices inserted or updated successfully")
    except Exception as e:
        print("Error:", e)

insert_prices_task = PythonOperator(
    task_id='insert_or_update_daily_fuel_prices',
    python_callable=insert_or_update_daily_fuel_prices,
    dag=dag,
)

# Set task dependencies
create_gas_station_table_task >> create_fuel_price_table_task >> insert_prices_task
