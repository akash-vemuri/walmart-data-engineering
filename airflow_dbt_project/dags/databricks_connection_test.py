from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime


def test_databricks_connection():

    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()

    user = w.current_user.me()

    print(f"Connected to Databricks as: {user.user_name}")


with DAG(
    dag_id="databricks_connection_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    test_connection = PythonOperator(
        task_id="test_databricks_connection",
        python_callable=test_databricks_connection,
    )