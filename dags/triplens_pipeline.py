from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id='triplens_countries_pipeline',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['triplens']
)
def triplens_pipeline():

    @task
    def extract():
        print("Extracting countries...")

    @task
    def load():
        print("Loading to Snowflake...")

    @task
    def transform():
        print("Running DBT...")

    extract() >> load() >> transform()

triplens_pipeline()