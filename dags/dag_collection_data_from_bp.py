from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import psycopg2

class Location:
    def __init__(self, location_id, name, lat, lng, site_brand, country_code, address, city, state, postcode, telephone):
        self.location_id = location_id
        self.name = name
        self.lat = lat
        self.lng = lng
        self.site_brand = site_brand
        self.country_code = country_code
        self.address = address
        self.city = city
        self.state = state
        self.postcode = postcode
        self.telephone = telephone

def get_remote_json():
    remote_url = "https://bpretaillocator.geoapp.me/api/v1/locations/within_bounds?sw%5B%5D=-46.8&sw%5B%5D=160.1&ne%5B%5D=-33.2&ne%5B%5D=-177.6&autoload=true&travel_mode=driving&avoid_tolls=false&avoid_highways=false&show_stations_on_route=true&corridor_radius=5&format=json"
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
    json_data = get_remote_json()

    conn = psycopg2.connect(database="p_database", user="root", password="password", host="host.docker.internal", port="5433")
    cur = conn.cursor()

    for location_json in json_data:
        location = Location(
            location_json['id'],
            location_json['name'],
            location_json['lat'],
            location_json['lng'],
            location_json['site_brand'],
            location_json['country_code'],
            location_json['address'],
            location_json['city'],
            location_json['state'],
            location_json['postcode'],
            location_json['telephone']
        )
        cur.execute("SELECT * FROM gas_station WHERE location_id = %s", (location.location_id,))
        if cur.fetchone() is None:
            cur.execute("INSERT INTO gas_station (location_id, brand_name, location_name, latitude, longitude, address_line1, city, state_province, postal_code, country) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (location.location_id, location.site_brand, location.name, location.lat, location.lng, location.address, location.city, location.state, location.postcode, location.country_code))
            print("Inserted data for location:", location.location_id)
        else:
            print("Data already exists for location:", location.location_id)
    conn.commit()
    conn.close()

default_args = {
    'owner': 'Aemon',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 12),
    'retries': 5,
}

dag = DAG(
    'collection_data_from_bp_v1',
    default_args=default_args,
    description='Collection remote data from BP and store into PostgreSQL',
    schedule_interval='0 0 * * *', 
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

task_get_remote_json >> task_check_and_insert_data
