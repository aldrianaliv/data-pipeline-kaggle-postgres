
# Requirement
Docker, Docker Compose, Python, DBeaver/PgAdmin/Adminer and Code Editor.

# Airflow - Docker
## Docker Setup for Airflow
1. First download the docker-compose file by running this command in the terminal.

> curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.0.6/docker-compose.yaml'

2. docker compose up airflow-init (initialize database for airflow)
3. docker-compose up -d (run docker images)
4. docker ps  (to check status of images)
5. Create an .env file and insert `AIRFLOW_UID=50000` 


# Pipeline Architecture 

![image](images\Pipeline-Kaggle-Postgre.drawio.png)

Using Docker as the baseline of the architecture to run Airflow. \
Streams data from Kaggle using Kaggle Hub (Uber Ride Data) to Postgres Database. \
Implemented database schemas (STG and SOR) to handles and prepare different stages of data. \


