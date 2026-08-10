from airflow.models import DagBag


def test_dag_loaded():
    dagbag = DagBag(include_examples=False)

    assert "sales_pipeline" in dagbag.dags
