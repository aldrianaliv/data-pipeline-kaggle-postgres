import datetime
from airflow.sdk import DAG,task
from airflow.providers.standard.operators.python import PythonOperator
rom airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
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
    df.to_csv(file_path, index=False) # Example of Data Lake Operation (GCS, S3, ADLS, etc.)

    return file_path

def check_data(ti, **kwargs):
    import pandas as pd
    file_path = ti.xcom_pull(task_ids="get_data")

    if file_path and file_path.strip():
        print("Number of columns in data:", df.shape[1])
        return 'file_path is not empty:', file_path
    else:
        return "file_path is empty!"


create_stg_schema = SQLExecuteQueryOperator(
        task_id="create_schema_stg",
        conn_id="postgressql_conn",
        sql="""
            CREATE SCHEMA IF NOT EXISTS STG; 
            """,
    )
create_sor_schema = SQLExecuteQueryOperator(
        task_id="create_schema_sor",
        conn_id="postgressql_conn",
        sql="""
            CREATE SCHEMA IF NOT EXISTS SOR;
            """,
    )

create_stg_uber_data = SQLExecuteQueryOperator(
        task_id="create_stg_table_uber_data",
        conn_id="postgressql_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS STG.uber_data (
                Request_id VARCHAR(50),
                Pickup_point VARCHAR(10),
                Driver_id VARCHAR(50),
                Status VARCHAR(10),
                Request_time TIMESTAMP,
                Drop_time TIMESTAMP,
                Fare FLOAT,
                Distance FLOAT,
                Rider_id VARCHAR(50)
            );
            """,
    )
create_sor_uber_data = SQLExecuteQueryOperator(
        task_id="create_sor_table_uber_data",
        conn_id="postgressql_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS SOR.uber_data (
                Request_id VARCHAR(50),
                Pickup_point VARCHAR(10),
                Driver_id VARCHAR(50),
                Status VARCHAR(10),
                Request_time TIMESTAMP,
                Drop_time TIMESTAMP,
                Fare FLOAT,
                Distance FLOAT,
                Rider_id VARCHAR(50)
            );
            """,
)
