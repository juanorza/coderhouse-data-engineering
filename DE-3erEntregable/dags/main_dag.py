from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.decorators import task

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

from python_files.main import main


with DAG(
    "main",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "owner": "JuanOrza",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="DAG simple para la ejecución del script main",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=True,
) as dag:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = PythonOperator(task_id="prueba1", python_callable=main)

    t2 = PythonOperator(task_id="prueba2", python_callable=todo_ok)

    t1 >> t2
