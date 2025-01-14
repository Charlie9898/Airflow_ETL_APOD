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
    ## endpoint: https://api.nasa.gov/planetary/apod?api_key=U7TB6zPprKEWtKIErGulnhcr0INu4TMrcOPj6AGG
    extract_apod=SimpleHttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api', #connection ID defined in Airflow for NASA api
        endpoint='planetary/apod', #NASA api endpoint for APOD
        method='GET',
        data={"api_key":"{{conn.nasa_api.extra_dejson.api_key}}"}, # Use the API key from the connection
        response_filter=lambda response:response.json(), # convert responses to json
    )

    ## task 3: transform the data (pick the data need to save)
    @task
    def transform_apod_data(response):
        apod_data={
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }
        return apod_data

    ## task 4: load the data into postgres SQL
    @task
    def load_data_to_postgres(apod_data):
        postgres_hook=PostgresHook(postgres_conn_id='my_postgres_connection')
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """

        postgres_hook.run(insert_query,parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']))

    ## task 5: verify the data DBViewer


    ## task 6: define the task dependency
    # Extract
    create_table() >> extract_apod
    api_response=extract_apod.output
    #transform
    transformed_data=transform_apod_data(api_response)
    #load
    load_data_to_postgres(transformed_data)