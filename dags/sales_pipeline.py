
from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime


def extract_data():
    print("Extrayendo datos...")


def transform_data():
    print("Transformando datos...")


def load_data():
    print("Cargando datos...")


with DAG(
    dag_id="sales_pipeline",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["data-engineering"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_data,
    )

    extract >> transform >> load