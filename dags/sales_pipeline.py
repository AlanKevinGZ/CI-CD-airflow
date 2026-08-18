from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from google.cloud import bigquery
from pendulum import datetime

PROJECT_ID = "mi-dw-123456"
DATASET_ID = "online_retail"
TABLE_ID = "raw_online_retail"

TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

def extract_data():
    project_dir = Path(__file__).resolve().parent.parent
    data_path = project_dir / "raw" / "data" / "Online Retail.xlsx"
    df = pd.read_excel(data_path)

    df["InvoiceNo"] = df["InvoiceNo"].astype(str)
    df["StockCode"] = df["StockCode"].astype(str)
    df["Description"] = df["Description"].astype("string")

    processed_dir = project_dir /  "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    df.to_parquet(processed_dir / "Online_Retail.parquet", index=False)
    

def load_data():
    project_dir = Path(__file__).resolve().parent.parent
    data_path = project_dir / "processed" / "Online_Retail.parquet"
    df = pd.read_parquet(data_path)
    hook = BigQueryHook( gcp_conn_id="google_cloud_default")
    client = hook.get_client()

    job_config = bigquery.LoadJobConfig( write_disposition="WRITE_TRUNCATE" )
    job = client.load_table_from_dataframe(df, TABLE_REF, job_config=job_config)
    job.result()
 


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


    load = PythonOperator(
        task_id="load",
        python_callable=load_data,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /usr/local/airflow/dbt_project && dbt run",
   )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /usr/local/airflow/dbt_project && dbt test",
    )

    extract >> load >> dbt_run >> dbt_test