import json
import os
import boto3
import snowflake.connector
from dotenv import load_dotenv
load_dotenv()

def get_latest_file_from_minio():
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
    )
    bucket = os.getenv("MINIO_BUCKET")
    objects = s3.list_objects_v2(Bucket=bucket, Prefix="countries/")
    files = sorted(objects["Contents"], key=lambda x: x["LastModified"], reverse=True)
    latest = files[0]["Key"]
    print(f"Reading latest file: {latest}")
    response = s3.get_object(Bucket=bucket, Key=latest)
    data = json.loads(response["Body"].read().decode("utf-8"))
    return data

def load_to_snowflake(data):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="RAW"
    )
    cursor = conn.cursor()

    # Create raw table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS RAW.COUNTRIES_RAW (
            id NUMBER AUTOINCREMENT PRIMARY KEY,
            raw_data VARIANT,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """)

    # Insert each country as a JSON row
    for country in data:
        cursor.execute(
            "INSERT INTO RAW.COUNTRIES_RAW (raw_data) SELECT PARSE_JSON(%s)",
            (json.dumps(country),)
        )

    conn.commit()
    print(f"Loaded {len(data)} countries into Snowflake RAW.COUNTRIES_RAW")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    data = get_latest_file_from_minio()
    load_to_snowflake(data)