from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import psycopg2

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

def get_remote_json():
    remote_url = "https://www.mobil.co.nz/en-NZ/api/RetailLocator/GetRetailLocations?Latitude1=-56.26107986344378&Latitude2=-23.713997579307097&Longitude1=135.9804712&Longitude2=-145.9726538&DataSource=RetailGasStations&Country=NZ&ResultLimit=10000"
    try:
        response = requests.get(remote_url)
        if response.status_code == 200:
            json_data = response.json()
            return json_data
        else:
            print("Error: HTTP status code", response.status_code)
    except Exception as e:
        print("Error:", e)

def check_and_insert_data():
    remote_data = get_remote_json()

    conn = psycopg2.connect(database="p_database", user="root", password="password", host="host.docker.internal", port="5433")
    cur = conn.cursor()

    for location_json in remote_data['Locations']:
        cur.execute("SELECT * FROM gas_station WHERE location_id = %s", (location_json['LocationID'],))
        if cur.rowcount == 0:
            cur.execute("INSERT INTO gas_station (location_id, brand_name, location_name, latitude, longitude, address_line1, city, state_province, postal_code, country) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (location_json['LocationID'], location_json['BrandName'], location_json['LocationName'], location_json['Latitude'], location_json['Longitude'], location_json['AddressLine1'], location_json['City'], location_json['StateProvince'], location_json['PostalCode'], location_json['Country']))
            print("Inserted data for location:", location_json['LocationID'])
        else:
            print("Data already exists for location:", location_json['LocationID'])

    conn.commit()
    conn.close()

default_args = {
    'owner': 'Aemon',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 12),
    'retries': 5,
}

dag = DAG(
    'collection_data_from_mobil_v5',
    default_args=default_args,
    description='Collection remote data from Mobil and store into PostgreSQL',
    schedule_interval='0 0 * * *', 
)

task_create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql=create_table_sql,
    dag=dag,
)

task_get_remote_json = PythonOperator(
    task_id='get_remote_json',
    python_callable=get_remote_json,
    dag=dag,
)

task_check_and_insert_data = PythonOperator(
    task_id='check_and_insert_data',
    python_callable=check_and_insert_data,
    dag=dag,
)

task_create_table >> task_get_remote_json >> task_check_and_insert_data
