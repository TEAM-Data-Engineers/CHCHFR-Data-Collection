from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2
import requests
from entities import GasStation

PG_DATABASE = "p_database"
PG_USER = "root"
PG_PASSWORD = "password"
PG_HOST = "host.docker.internal"
PG_PORT = "5433"

API_KEY = Variable.get("google_api_key")
LOCATION = "-43.5252283,172.6375393"
RADIUS = 5000000
KEYWORD = "Pak'nSave"
TYPES = "gas_station"

def create_connection():
    conn = psycopg2.connect(
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    )
    return conn

def fetch_gas_stations():
    url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?key={API_KEY}&location={LOCATION}&radius={RADIUS}&keyword={KEYWORD}&types={TYPES}"
    response = requests.get(url)
    if response.status_code == 200:
        print("The number of gas stations found: ", len(response.json().get("results", [])))
        return response.json().get("results", [])
    else:
        return []

def get_value_after_last_comma(string):
    if "," in string:
        return string.split(",")[-1].strip()
    else:
        return ""

def fetch_and_insert_gas_stations(**kwargs):
    conn = create_connection()
    cur = conn.cursor()
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
            cur.execute("INSERT INTO gas_station (location_id, brand_name, location_name, latitude, longitude, address_line1, city, country) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (gas_station.location_id, gas_station.brand_name, gas_station.location_name, gas_station.latitude, gas_station.longitude, gas_station.address_line1, gas_station.city, gas_station.country))
            conn.commit()
            print("Inserted data for location:", gas_station.location_id)
    cur.close()
    conn.close()

default_args = {
    "owner": "Aemon",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5
}

dag = DAG(
    "collection_data_from_paknsave_v10",
    default_args=default_args,
    description="Fetch and insert gas stations into PostgreSQL database",
    schedule_interval="@daily",
    catchup=False
)

task_fetch_and_insert_gas_stations = PythonOperator(
    task_id="fetch_and_insert_gas_stations",
    python_callable=fetch_and_insert_gas_stations,
    dag=dag
)

task_fetch_and_insert_gas_stations
