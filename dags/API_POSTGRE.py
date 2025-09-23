import datetime
from airflow.sdk import DAG,task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum


dag = DAG(
    dag_id="API_POSTGRE",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
)

def get_data(**kwargs):
    import kagglehub
    from kagglehub import KaggleDatasetAdapter

    file_path = "dags/data/uber-data.csv"

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
    if not file_path or not file_path.strip():
        raise ValueError("File Path is empty!")
    df = pd.read_csv(file_path)
    print("Number of columns in data:", df.shape[1])
    return file_path


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

def create_stg_table_from_csv(ti, **kwargs):
    import pandas as pd
    file_path = ti.xcom_pull(task_ids="get_data")
    print(file_path)
    if not file_path or not file_path.strip():
        raise ValueError("No file path provided!")

    df = pd.read_csv(file_path)
    columns = df.columns.tolist()
    
    columns_sql = ",\n    ".join([f'"{col}" TEXT' for col in columns])
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS STG.uber_data (
        {columns_sql}
    );
    """

    hook = PostgresHook(postgres_conn_id="postgressql_conn")
    hook.run(create_table_sql)
    print("STG.uber_data table created with columns:", columns)


create_stg_uber_data = PythonOperator(
    task_id="create_stg_table_uber_data",
    python_callable=create_stg_table_from_csv,
    dag=dag,
)

def load_csv_to_stg(ti, **kwargs):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
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

create_sor_uber_data = SQLExecuteQueryOperator(
        task_id="create_sor_table_uber_data",
        conn_id="postgressql_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS SOR.uber_ride (
            booking_date DATE,
            booking_time TIME,
            booking_id VARCHAR(50) UNIQUE,
            booking_status VARCHAR(50),
            customer_id VARCHAR(50),
            vehicle_type VARCHAR(50),
            pickup_location VARCHAR(100),
            drop_location VARCHAR(100),
            avg_vtat DECIMAL,
            avg_ctat DECIMAL,
            cancelled_rides_by_customer DECIMAL,
            reason_for_cancelling_by_customer VARCHAR(255),
            cancelled_rides_by_driver DECIMAL,
            driver_cancellation_reason VARCHAR(255),
            incomplete_rides DECIMAL,
            incomplete_rides_reason VARCHAR(255),
            booking_value DECIMAL,
            ride_distance DECIMAL,
            driver_ratings DECIMAL,
            customer_rating DECIMAL,
            payment_method VARCHAR(50)
            );
            """,
)

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

load_csv_to_stg = PythonOperator(
    task_id="load_csv_to_stg",
    python_callable=load_csv_to_stg,
    dag=dag,
)

load_stg_to_sor = SQLExecuteQueryOperator(
        task_id="load_stg_to_sor",
        conn_id="postgressql_conn",
        sql="""
        INSERT INTO sor.uber_ride (
            booking_date,
            booking_time,
            booking_id,
            booking_status,
            customer_id,
            vehicle_type,
            pickup_location,
            drop_location,
            avg_vtat,
            avg_ctat,
            cancelled_rides_by_customer,
            reason_for_cancelling_by_customer,
            cancelled_rides_by_driver,
            driver_cancellation_reason,
            incomplete_rides,
            incomplete_rides_reason,
            booking_value,
            ride_distance,
            driver_ratings,
            customer_rating,
            payment_method
    )
        SELECT *
        FROM (
            SELECT DISTINCT ON ("Booking ID")
            TO_DATE("Date", 'YYYY-MM-DD') AS booking_date,
            TO_TIMESTAMP("Time", 'HH24:MI:SS')::TIME AS booking_time,
            "Booking ID" AS booking_id,
            "Booking Status" AS booking_status,
            "Customer ID" AS customer_id,
            "Vehicle Type" AS vehicle_type,
            "Pickup Location" AS pickup_location,
            "Drop Location" AS drop_location,
            NULLIF("Avg VTAT", '')::DECIMAL AS avg_vtat,
            NULLIF("Avg CTAT", '')::DECIMAL AS avg_ctat,
            NULLIF("Cancelled Rides by Customer", '')::DECIMAL AS cancelled_rides_by_customer,
            "Reason for cancelling by Customer" AS reason_for_cancelling_by_customer,
            NULLIF("Cancelled Rides by Driver", '')::DECIMAL AS cancelled_rides_by_driver,
            "Driver Cancellation Reason" AS driver_cancellation_reason,
            NULLIF("Incomplete Rides", '')::DECIMAL AS incomplete_rides,
            "Incomplete Rides Reason" AS incomplete_rides_reason,
            NULLIF("Booking Value", '')::DECIMAL AS booking_value,
            NULLIF("Ride Distance", '')::DECIMAL AS ride_distance,
            NULLIF("Driver Ratings", '')::DECIMAL AS driver_ratings,
            NULLIF("Customer Rating", '')::DECIMAL AS customer_rating,
            "Payment Method" AS payment_method
        FROM stg.uber_data
        ORDER BY "Booking ID", "Date" DESC
        ) AS deduped
        ON CONFLICT (booking_id) 
        DO UPDATE SET
            booking_date = EXCLUDED.booking_date,
            booking_time = EXCLUDED.booking_time,
            booking_status = EXCLUDED.booking_status,
            customer_id = EXCLUDED.customer_id,
            vehicle_type = EXCLUDED.vehicle_type,
            pickup_location = EXCLUDED.pickup_location,
            drop_location = EXCLUDED.drop_location,
            avg_vtat = EXCLUDED.avg_vtat,
            avg_ctat = EXCLUDED.avg_ctat,
            cancelled_rides_by_customer = EXCLUDED.cancelled_rides_by_customer,
            reason_for_cancelling_by_customer = EXCLUDED.reason_for_cancelling_by_customer,
            cancelled_rides_by_driver = EXCLUDED.cancelled_rides_by_driver,
            driver_cancellation_reason = EXCLUDED.driver_cancellation_reason,
            incomplete_rides = EXCLUDED.incomplete_rides,
            incomplete_rides_reason = EXCLUDED.incomplete_rides_reason,
            booking_value = EXCLUDED.booking_value,
            ride_distance = EXCLUDED.ride_distance,
            driver_ratings = EXCLUDED.driver_ratings,
            customer_rating = EXCLUDED.customer_rating,
            payment_method = EXCLUDED.payment_method;
        """,
)

# This is an example of incremental load (using INSERT ... ON CONFLICT)
# If you want to do a full load, you can use TRUNCATE + INSERT (refresh)

get_data >> check_data >> create_stg_schema >> create_sor_schema >> create_stg_uber_data >> load_csv_to_stg >> create_sor_uber_data >> load_stg_to_sor  



