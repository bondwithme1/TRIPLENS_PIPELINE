import requests
import json
import os
import boto3
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

API_KEY = os.getenv("RESTCOUNTRIES_API_KEY")
API_URL = "https://api.restcountries.com/countries/v5"

def extract_countries():
    print("Fetching data from REST Countries API...")
    all_countries = []
    offset = 0

    while True:
        response = requests.get(
            f"{API_URL}?limit=25&offset={offset}",
            headers={"Authorization": f"Bearer {API_KEY}"}
        )
        if response.status_code != 200:
            raise Exception(f"API call failed: {response.status_code} - {response.text}")

        payload = response.json()
        print(payload)
        batch = (payload.get("data") or {}).get("objects") or []
        if not batch:
            break
        all_countries.extend(batch)
        offset += len(batch)
        print(f"  Fetched {len(all_countries)} so far...")

    print(f"Successfully fetched {len(all_countries)} countries.")
    return all_countries

def save_locally(data):
    os.makedirs("data/raw", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"data/raw/countries_{timestamp}.json"
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Saved to {filename}")
    return filename, timestamp

def upload_to_minio(filename, timestamp):
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
    )
    bucket = os.getenv("MINIO_BUCKET")
    key = f"countries/countries_{timestamp}.json"
    s3.upload_file(filename, bucket, key)
    print(f"Uploaded to MinIO: {bucket}/{key}")

if __name__ == "__main__":
    data = extract_countries()
    filename, timestamp = save_locally(data)
    upload_to_minio(filename, timestamp)
    

