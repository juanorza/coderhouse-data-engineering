from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.decorators import task

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

from project_files.start_db import start_db
from project_files.extract import extraction_task
from project_files.load import transform_and_load_task
from project_files.alerta import lanzar_alerta_task

DAILY_VARIABLES = [
    "weathercode",
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "sunrise",
    "sunset",
]

EXTRACT_CONFIG = {
    "id": "ciudades_daily",
    "frequency": "daily",  # {daily, hourly}
    "query_params": {
        "start_date": None,
        "end_date": None,
        "daily": ",".join(DAILY_VARIABLES),
        "latitude": None,
        "longitude": None,
        "timezone": None,
    },
    "city_id": None,
}

LOAD_CONFIG = {"table_name": "mediciones_diarias", "if_exists": "append"}


with DAG(
    "start_database_dag",
    default_args={
        "owner": "JuanOrza",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="DAG para la creación a cero de las tablas necesarias para almacenar los datos descargados. Se ejecuta una única vez, previo a al DAG principal",
    schedule_interval="@once",
    start_date=datetime(2000, 1, 1),
    catchup=True,
) as dag0:
    task0 = PythonOperator(task_id="start_database_task", python_callable=start_db)
    task0


with DAG(
    "main_dag",
    default_args={
        "owner": "JuanOrza",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="DAG para la ejecución del script principal. Consta de tres tareas: extracción, transformación y carga, y lanzar alerta.",
    schedule_interval="@daily",
    start_date=datetime(2023, 3, 1),
    end_date=datetime(2023, 4, 30),
    catchup=True,
) as dag:
    filename = extraction_task(extract_config=EXTRACT_CONFIG.copy())
    transform_and_load_task(csv_filename=filename, load_config=LOAD_CONFIG)

    lanzar_alerta_task(csv_filename=filename)
