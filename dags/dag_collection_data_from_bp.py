from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import requests
import logging
from entities.gas_station_schema import GasStation

# Default arguments for the DAG
default_args = {
    'owner': 'Aemon',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 12),
    'retries': 5,
}

# Define the DAG
dag = DAG(
    'collection_data_from_bp_v1',
    default_args=default_args,
    description='Collection remote data from BP and store into PostgreSQL',
    schedule_interval='0 0 * * *',  # Run daily at midnight
)

def get_remote_json():
    remote_url = "https://bpretaillocator.geoapp.me/api/v1/locations/within_bounds?sw%5B%5D=-46.8&sw%5B%5D=160.1&ne%5B%5D=-33.2&ne%5B%5D=-177.6&autoload=true&travel_mode=driving&avoid_tolls=false&avoid_highways=false&show_stations_on_route=true&corridor_radius=5&format=json"
    try:
        response = requests.get(remote_url)
        response.raise_for_status()
        json_data = response.json()
        logging.info("Successfully fetched remote JSON data")
        return json_data
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching remote JSON data: {e}")
        raise

def check_and_insert_data():
    json_data = get_remote_json()

    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        for location_json in json_data:
            location = GasStation(
                location_id=location_json['id'],
                brand_name=location_json['site_brand'],
                location_name=location_json['name'],
                latitude=location_json['lat'],
                longitude=location_json['lng'],
                address_line1=location_json['address'],
                city=location_json['city'],
                state_province=location_json['state'],
                postal_code=location_json['postcode'],
                country=location_json['country_code']
            )
            cur.execute("SELECT COUNT(*) FROM gas_station WHERE location_id = %s", (location.location_id,))
            count = cur.fetchone()[0]
            if count == 0:
                cur.execute("""
                    INSERT INTO gas_station (
                        location_id, brand_name, location_name, latitude, longitude, 
                        address_line1, city, state_province, postal_code, country
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    location.location_id, location.brand_name, location.location_name,
                    location.latitude, location.longitude, location.address_line1,
                    location.city, location.state_province, location.postal_code, location.country
                ))
                logging.info(f"Inserted data for location: {location.location_id}")
            else:
                logging.info(f"Data already exists for location: {location.location_id}")
        conn.commit()
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
        logging.info("Database connection closed")

# Define the tasks
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

# Set task dependencies
task_get_remote_json >> task_check_and_insert_data
