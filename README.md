# CHCH-Fuel-Recommendator Data Collection

This repository contains DAGs for the CHCH-Fuel-Recommendator project, which collects data from various fuel stations and stores it in a PostgreSQL database. The project is structured to run daily data collection and update fuel prices.

## Project Structure

```bash
CHCH-Fuel-Recommendator-Data-Collection/
├── dags/
│   ├── __pycache__/
│   ├── entities/
│   │   ├── __pycache__/
│   │   ├── __init__.py
│   │   └── gas_station_schema.py
│   ├── dag_collection_data_from_bp.py
│   ├── dag_collection_data_from_mobil.py
│   ├── dag_collection_data_from_paknsave.py
│   ├── dag_collection_data_from_z.py
│   ├── dag_create_gas_station_table.py
│   └── dag_daily_fuel_price_generation.py
├── .gitignore
├── README.md
├── requirements.txt
└── webserver_config.py
```

## Prerequisites

- Python 3.7+
- Apache Airflow 2.0+
- PostgreSQL database

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/TEAM-Data-Engineers/CHCH-Fuel-Recommendator-Data-Collection.git
cd CHCH-Fuel-Recommendator-Data-Collection
```

### 2. Create and Activate a Virtual Environment

```bash
python -m venv airflow_env
source airflow_env/bin/activate
```

### 3. Install the Required Packages

```bash
pip install -r requirements.txt
```

### 4. Initialize Airflow

```bash
export AIRFLOW_HOME=$(pwd)
airflow db init
```

### 5. Set Up Airflow Connections

Set up the necessary Airflow connections for PostgreSQL. You can do this via the Airflow UI or the command line.

For example, to set up a PostgreSQL connection:

```bash
airflow connections add 'postgres_default' \
    --conn-uri 'postgresql+psycopg2://username:password@hostname:5432/dbname'
```

Replace `username`, `password`, `hostname`, `5432`, and `dbname` with your PostgreSQL credentials and details.

### 6. Run Airflow

Start the Airflow web server and scheduler:

```bash
airflow webserver -D
airflow scheduler -D
```

### 7. Configure Airflow Variables

Set the required Airflow variables, either via the Airflow UI or by using the CLI.

For example, to set a variable:

```bash
airflow variables set google_api_key 'your_google_api_key'
airflow variables set PG_DATABASE 'your_pg_database'
airflow variables set PG_USER 'your_pg_user'
airflow variables set PG_PASSWORD 'your_pg_password'
airflow variables set PG_HOST 'your_pg_host'
airflow variables set PG_PORT 'your_pg_port'
```

## DAGs Overview

### 1. Create Gas Station Table DAG

**File:** `dag_create_gas_station_table.py`

This DAG creates the `gas_station` table in the PostgreSQL database.

**Schedule:** Once

### 2. Data Collection DAGs

These DAGs collect data from various fuel station APIs and store the data in the PostgreSQL database.

- **BP:** `dag_collection_data_from_bp.py`
- **Mobil:** `dag_collection_data_from_mobil.py`
- **Pak'nSave:** `dag_collection_data_from_paknsave.py`
- **Z Energy:** `dag_collection_data_from_z.py`

**Schedule:** Daily at 1 AM

### 3. Daily Fuel Price Generation DAG

**File:** `dag_daily_fuel_price_generation.py`

This DAG generates daily fuel prices for gas stations.

**Schedule:** Daily at 2 AM

## Logging

Logs for each task instance are stored in the `logs` directory. Ensure this directory is listed in `.gitignore` to prevent it from being included in the repository.
