from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import json
import logging
from entities.gas_station_schema import GasStation

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
    schedule_interval='0 1 * * *',  # Daily at 1 AM
    catchup=False
)

wait_for_table_creation = ExternalTaskSensor(
    task_id='wait_for_table_creation',
    external_dag_id='create_gas_station_table',
    external_task_id='create_gas_station_table',
    timeout=600,
    poke_interval=30,
    mode='poke',
    dag=dag,
)

def get_station_data():
    url = "https://www.z.co.nz/find-a-station"
    try:
        logging.info(f"Fetching data from URL: {url}")
        response = requests.get(url)
        response.raise_for_status()
        html_content = response.content
        logging.info("Successfully fetched HTML content")
        soup = BeautifulSoup(html_content, "html.parser")
        div_locator = soup.find("div", class_="locator")
        if div_locator:
            div_data_props = div_locator.find("div", attrs={"data-props": True})
            if div_data_props:
                data_props_json = json.loads(div_data_props["data-props"])
                stations_data = data_props_json.get("stations", [])
                logging.info("Successfully fetched and parsed station data")
                return stations_data
            else:
                logging.error("Cannot find data-props attribute in div element")
                raise Exception("Cannot find data-props attribute in div element")
        else:
            logging.error("Cannot find locator class in div element")
            raise Exception("Cannot find locator class in div element")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching station data: {e}")
        raise
    except Exception as e:
        logging.error(f"Error parsing station data: {e}")
        raise

def check_and_insert_data():
    logging.info("Starting check_and_insert_data task")
    stations_data = get_station_data()

    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        for station_data in stations_data:
            location_id = station_data.get('externalID')
            logging.info(f"Processing station: {location_id}")
            cur.execute("SELECT COUNT(*) FROM gas_station WHERE location_id = %s", (location_id,))
            count = cur.fetchone()[0]
            if count == 0:
                gas_station = GasStation(
                    location_id=location_id,
                    brand_name="Z Energy",
                    location_name=station_data.get("name"),
                    latitude=station_data.get("latitude"),
                    longitude=station_data.get("longitude"),
                    address_line1=station_data.get("address"),
                    city=station_data.get("city"),
                    state_province=station_data.get("region"),
                    postal_code=station_data.get("postcode"),
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
        logging.info("Data insertion completed successfully")
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
        logging.info("Database connection closed")

# Define the tasks
task_check_and_insert_data = PythonOperator(
    task_id='check_and_insert_data',
    python_callable=check_and_insert_data,
    dag=dag,
)

# Define task dependencies
wait_for_table_creation >> task_check_and_insert_data
