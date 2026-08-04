from airflow.decorators import dag, task
from datetime import datetime
import os


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
        import requests
        import json
        import boto3
        from datetime import datetime as dt

        response = requests.get("https://restcountries.com/v3.1/all")
        data = response.json()
        print(f"Fetched {len(data)} countries")

        s3 = boto3.client("s3",
            endpoint_url="http://host.docker.internal:9000",
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"))

        timestamp = dt.now().strftime("%Y%m%d_%H%M%S")
        key = f"countries/countries_{timestamp}.json"
        s3.put_object(
            Bucket=os.getenv("MINIO_BUCKET"),
            Key=key,
            Body=json.dumps(data))
        print(f"Uploaded to MinIO: {key}")
        return key

    @task
    def load(minio_key: str):
        import json
        import boto3
        import snowflake.connector

        s3 = boto3.client("s3",
            endpoint_url="http://host.docker.internal:9000",
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"))

        response = s3.get_object(Bucket=os.getenv("MINIO_BUCKET"), Key=minio_key)
        data = json.loads(response["Body"].read().decode("utf-8"))
        print(f"Read {len(data)} countries from MinIO")

        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema="RAW")

        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE IF EXISTS RAW.COUNTRIES_RAW")
        for country in data:
            cursor.execute(
                "INSERT INTO RAW.COUNTRIES_RAW (raw_data) SELECT PARSE_JSON(%s)",
                (json.dumps(country),))
        conn.commit()
        cursor.close()
        conn.close()
        print("Loaded to Snowflake successfully")

    @task
    def transform():
        import subprocess
        result = subprocess.run(
            [
                "dbt", "run",
                "--project-dir", "/usr/local/airflow/include/dbt_project",
                "--profiles-dir", "/usr/local/airflow/include"
            ],
            capture_output=True,
            text=True,
            env={**os.environ, "DBT_PROFILES_DIR": "/usr/local/airflow/include"}
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(f"DBT run failed:\n{result.stderr}")
        print("DBT transformation complete")

    minio_key = extract()
    load_task = load(minio_key)
    transform_task = transform()
    load_task >> transform_task


triplens_pipeline()