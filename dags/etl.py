from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json

## Define the DAG

with DAG(
    dag_id='nasa_postgres',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    ## task 1: create the table if it does not exist
    @task
    def create_table():
        ## initialize the Postgreshook
        postgres_hook=PostgresHook(postgres_conn_id="my_postgres_connection")

        ## SQL query to create the table
        create_table_query="""
        CREATE TABLE IF NOT EXIST apod_data(
        id SERIAL PRIMARY KEY,
        title VARCHAR(255),
        explanation TEXT,
        url TEXT,
        date DATE,
        media_type VARCHAR(50)
        );
        """

        ## Execute the query
        postgres_hook.run(create_table_query)

        
    ## task 2: extract the NASA API data (APOD) [Extract pipeline]

    ## task 3: transform the data (pick the data need to save)

    ## task 4: load the data into postgres SQL

    ## task 5: verify the data DBViewer

    ## task 6: define the task dependency