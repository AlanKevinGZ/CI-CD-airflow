from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from pendulum import datetime

with DAG(
    dag_id="test_bigquery",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["bigquery"],
) as dag:

    test_query = BigQueryInsertJobOperator(
        task_id="test_query",
        configuration={
            "query": {
                "query": "SELECT 1 AS test",
                "useLegacySql": False,
            }
        },
        location="US",
        gcp_conn_id="google_cloud_default",
    )