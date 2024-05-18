from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import requests
import logging
from entities.gas_station_schema import GasStation

# Database connection parameters
PG_DATABASE = Variable.get("PG_DATABASE", default_var="p_database")
PG_USER = Variable.get("PG_USER", default_var="root")
PG_PASSWORD = Variable.get("PG_PASSWORD", default_var="password")
PG_HOST = Variable.get("PG_HOST", default_var="host.docker.internal")
PG_PORT = Variable.get("PG_PORT", default_var="5433")

# Google Places API parameters
API_KEY = Variable.get("google_api_key")
LOCATION = "-43.5252283,172.6375393"
RADIUS = 5000000
KEYWORD = "Pak'nSave"
TYPES = "gas_station"

# Default arguments for the DAG
default_args = {
    "owner": "Aemon",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5
}

# Define the DAG
dag = DAG(
    "collection_data_from_paknsave_v10",
    default_args=default_args,
    description="Fetch and insert gas stations into PostgreSQL database",
    schedule_interval="@daily",
    catchup=False
)

def fetch_gas_stations():
    url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?key={API_KEY}&location={LOCATION}&radius={RADIUS}&keyword={KEYWORD}&types={TYPES}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        stations = response.json().get("results", [])
        logging.info(f"The number of gas stations found: {len(stations)}")
        return stations
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching gas stations: {e}")
        return []

def get_value_after_last_comma(string):
    if "," in string:
        return string.split(",")[-1].strip()
    return ""

def fetch_and_insert_gas_stations(**kwargs):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        for station in fetch_gas_stations():
            location_id = station.get("place_id")
            cur.execute("SELECT COUNT(*) FROM gas_station WHERE location_id = %s", (location_id,))
            count = cur.fetchone()[0]
            if count == 0:
                gas_station = GasStation(
                    location_id=location_id,
                    brand_name=station.get("name"),
                    location_name=station.get("name"),
                    latitude=station.get("geometry", {}).get("location", {}).get("lat"),
                    longitude=station.get("geometry", {}).get("location", {}).get("lng"),
                    address_line1=station.get("vicinity"),
                    city=get_value_after_last_comma(station.get("vicinity")),
                    state_province="",
                    postal_code="",
                    country="NZ"
                )
                cur.execute("""
                    INSERT INTO gas_station (
                        location_id, brand_name, location_name, latitude, longitude, 
                        address_line1, city, state_province, postal_code, country
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    gas_station.location_id, gas_station.brand_name, gas_station.location_name,
                    gas_station.latitude, gas_station.longitude, gas_station.address_line1,
                    gas_station.city, gas_station.state_province, gas_station.postal_code, gas_station.country
                ))
                logging.info(f"Inserted data for location: {location_id}")
            else:
                logging.info(f"Data already exists for location: {location_id}")
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
task_fetch_and_insert_gas_stations = PythonOperator(
    task_id="fetch_and_insert_gas_stations",
    python_callable=fetch_and_insert_gas_stations,
    dag=dag
)

task_fetch_and_insert_gas_stations
