from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import requests
import logging
from entities.gas_station_schema import GasStation

default_args = {
    "owner": "Aemon",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 12),
    "retries": 5,
}

dag = DAG(
    "collection_data_from_mobil_v1",
    default_args=default_args,
    description="Fetch and process data from Mobil daily",
    schedule_interval="0 0 * * *",  # Run daily at midnight
)


def get_remote_json():
    remote_url = "https://www.mobil.co.nz/en-NZ/api/RetailLocator/GetRetailLocations?Latitude1=-56.26107986344378&Latitude2=-23.713997579307097&Longitude1=135.9804712&Longitude2=-145.9726538&DataSource=RetailGasStations&Country=NZ&ResultLimit=10000"
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
    remote_data = get_remote_json()

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        for location_json in remote_data["Locations"]:
            location_id = location_json["LocationID"]
            cur.execute(
                "SELECT COUNT(*) FROM gas_station WHERE location_id = %s",
                (location_id,),
            )
            count = cur.fetchone()[0]
            if count == 0:
                gas_station = GasStation(
                    location_id=location_id,
                    brand_name=location_json["BrandName"],
                    location_name=location_json["LocationName"],
                    latitude=location_json["Latitude"],
                    longitude=location_json["Longitude"],
                    address_line1=location_json["AddressLine1"],
                    city=location_json["City"],
                    state_province=location_json["StateProvince"],
                    postal_code=location_json["PostalCode"],
                    country=location_json["Country"],
                )
                cur.execute(
                    """
                    INSERT INTO gas_station (
                        location_id, brand_name, location_name, latitude, longitude, 
                        address_line1, city, state_province, postal_code, country
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        gas_station.location_id,
                        gas_station.brand_name,
                        gas_station.location_name,
                        gas_station.latitude,
                        gas_station.longitude,
                        gas_station.address_line1,
                        gas_station.city,
                        gas_station.state_province,
                        gas_station.postal_code,
                        gas_station.country,
                    ),
                )
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
task_get_remote_json = PythonOperator(
    task_id="get_remote_json",
    python_callable=get_remote_json,
    dag=dag,
)

task_check_and_insert_data = PythonOperator(
    task_id="check_and_insert_data",
    python_callable=check_and_insert_data,
    dag=dag,
)

# Set task dependencies
task_get_remote_json >> task_check_and_insert_data
