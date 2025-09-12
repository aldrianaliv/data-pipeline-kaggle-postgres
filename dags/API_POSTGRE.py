import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum


DAG = DAG(
    dag_id="API_POSTGRE",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
)

def get_data(**kwargs):
    import kagglehub
    from kagglehub import KaggleDatasetAdapter

    file_path = "dags\data\uber-data.csv"

    df = kagglehub.dataset_load(
        KaggleDatasetAdapter.PANDAS, 
        "yashdevladdha/uber-ride-analytics-dashboard",
        "ncr_ride_bookings.csv"
    )
    print("Fetching data from API and storing in PostgreSQL")
    df.to_csv(file_path, index=False)

    return file_path

def check_data(ti, **kwargs):
    import pandas as pd
    file_path = ti.xcom_pull(task_ids="get_data")

    if file_path and file_path.strip():
        print("Number of columns in data:", df.shape[1])
        return 'file_path is not empty:', file_path
    else:
        return "file_path is empty!"


