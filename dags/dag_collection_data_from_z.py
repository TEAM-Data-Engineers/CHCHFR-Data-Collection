from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import json
import psycopg2
from psycopg2 import Error
from entities import GasStation

class Station:
    def __init__(self, name, externalID, address, suburb, city, postcode, region, latitude, longitude, type, type_slug, isPayAtPump247, phone):
        self.name = name
        self.externalID = externalID
        self.address = address
        self.suburb = suburb
        self.city = city
        self.postcode = postcode
        self.region = region
        self.latitude = latitude
        self.longitude = longitude
        self.type = type
        self.type_slug = type_slug
        self.isPayAtPump247 = isPayAtPump247
        self.phone = phone

default_args = {
    'owner': 'Aemon',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 12),
    'retries': 5,
}

dag = DAG(
    'collection_data_from_z_v3',
    default_args=default_args,
    description='A DAG to collect Z Energy station data and insert into PostgreSQL',
    schedule_interval='0 0 * * *',  # Daily at midnight
)

def get_station_data():
    url = "https://www.z.co.nz/find-a-station"
    response = requests.get(url)
    html_content = response.content
    soup = BeautifulSoup(html_content, "html.parser")
    div_locator = soup.find("div", class_="locator")
    if div_locator:
        div_data_props = div_locator.find("div", attrs={"data-props": True})
        if div_data_props:
            data_props_json = json.loads(div_data_props["data-props"])
            stations_data = data_props_json.get("stations", [])
            return stations_data
        else:
            raise Exception("Cannot find data-props attribute in div element")
    else:
        raise Exception("Cannot find locator class in div element")

def check_and_insert_data():
    stations_data = get_station_data()
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            database="p_database",
            user="root",
            password="password",
            host="host.docker.internal",
            port="5433"
        )
        cur = conn.cursor()
        
        # Iterate through stations data
        for station_data in stations_data:
            # Check if station with externalID already exists in the database
            sql = f"SELECT COUNT(*) FROM gas_station WHERE location_id = '{station_data.get('externalID')}'"
            cur.execute(sql)
            count = cur.fetchone()[0]
            if count == 0:
                # Insert station data into the database
                sql = """
                INSERT INTO gas_station (location_id, brand_name, location_name, latitude, longitude, address_line1, city, state_province, postal_code, country)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                values = (
                    station_data.get("externalID"),
                    "Z Energy",
                    station_data.get("name"),
                    station_data.get("latitude"),
                    station_data.get("longitude"),
                    station_data.get("address"),
                    station_data.get("city"),
                    station_data.get("region"),
                    station_data.get("postcode"),
                    "NZ"
                )

                cur.execute(sql, values)
                conn.commit()
                print(f"Inserted new station: {station_data.get('name')}")
            else:
                print(f"Station already exists: {station_data.get('name')}")
    except Error as e:
        print(f"Error: {e}")
    finally:
        # Close database connection
        if conn:
            cur.close()
            conn.close()

# Define tasks
get_data_task = PythonOperator(
    task_id='get_data',
    python_callable=get_station_data,
    dag=dag,
)

check_and_insert_task = PythonOperator(
    task_id='check_and_insert',
    python_callable=check_and_insert_data,
    dag=dag,
)

# Define task dependencies
get_data_task >> check_and_insert_task
