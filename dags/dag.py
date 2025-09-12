import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

import pendulum

with DAG(
    dag_id="a_test_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
) as dag:
    def print_hello():
        print("Hello, Airflow!")

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=print_hello,
)

    hello_task