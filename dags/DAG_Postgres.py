import datetime
from airflow.sdk import DAG,task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum


dag = DAG(
    dag_id="DAG_Postgres",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
)

def get_data(**kwargs):
    import kagglehub
    from kagglehub import KaggleDatasetAdapter
    import pandas as pd

    file_path = "dags/data/uber-data.csv"

    try:
        df = kagglehub.dataset_load(
            KaggleDatasetAdapter.PANDAS, 
            "yashdevladdha/uber-ride-analytics-dashboard",
            "ncr_ride_bookings.csv"
        )
        print("Fetching data from API and storing in CSV")
        df.to_csv(file_path, index=False)  # Example of Data Lake Operation (GCS, S3, etc.)
    except Exception as e:
        print(f"Failed to fetch from KaggleHub, using local file: {file_path}")
    
    return file_path

def check_data(ti, **kwargs):
    import pandas as pd

    file_path = ti.xcom_pull(task_ids="get_data")
    if not file_path or not file_path.strip():
        raise ValueError("File Path is empty!")
    df = pd.read_csv(file_path)
    print("Number of columns in data:", df.shape[1])
    return file_path

# def create_stg_table_from_csv(ti, **kwargs):
#     import pandas as pd
#     file_path = ti.xcom_pull(task_ids="get_data")
#     print(file_path)
#     if not file_path or not file_path.strip():
#         raise ValueError("No file path provided!")

#     df = pd.read_csv(file_path)
#     columns = df.columns.tolist()
    
#     columns_sql = ",\n    ".join([f'"{col}" TEXT' for col in columns])
#     create_table_sql = f"""
#     CREATE TABLE IF NOT EXISTS STG.uber_data (
#         {columns_sql}
#     );
#     """

#     hook = PostgresHook(postgres_conn_id="postgressql_conn")
#     hook.run(create_table_sql)
#     print("STG.uber_data table created with columns:", columns)


# create_stg_uber_data = PythonOperator(
#     task_id="create_stg_table_uber_data",
#     python_callable=create_stg_table_from_csv,
#     dag=dag,
# )


def load_csv_to_stg(ti, **kwargs):
    file_path = ti.xcom_pull(task_ids="get_data")
    print(file_path)

    if not file_path or not file_path.strip():
        raise ValueError("No file path provided!")
    
    hook = PostgresHook(postgres_conn_id="postgressql_conn")
    sql = f"""
        COPY STG.uber_data
        FROM STDIN
        WITH CSV HEADER
    """
    hook.copy_expert(sql=sql, filename=file_path)

    print("Data CSV berhasil dimuat ke STG.uber_data")

get_data = PythonOperator(
    task_id="get_data",
    python_callable=get_data,
    dag=dag,
)

check_data = PythonOperator(
    task_id="check_data",
    python_callable=check_data,
    dag=dag,
)

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
    sql= "models/create_stg_uber_data.sql",
)

load_csv_to_stg = PythonOperator(
    task_id="load_csv_to_stg",
    python_callable=load_csv_to_stg,
    dag=dag,
)

create_sor_uber_data = SQLExecuteQueryOperator(
        task_id="create_sor_table_uber_data",
        conn_id="postgressql_conn",
        sql="models/create_sor_uber_data.sql",
)
load_stg_to_sor = SQLExecuteQueryOperator(
        task_id="load_stg_to_sor",
        conn_id="postgressql_conn",
        sql="models/load_stg_to_sor.sql",
)


# This is an example of incremental load (using INSERT ... ON CONFLICT)
# If you want to do a full load, you can use TRUNCATE + INSERT (refresh)

get_data >> check_data >> [create_stg_schema, create_sor_schema] >> create_stg_uber_data >> load_csv_to_stg >> create_sor_uber_data >> load_stg_to_sor


