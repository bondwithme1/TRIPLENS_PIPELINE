def upload_to_s3(filename, timestamp):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )
    bucket = os.getenv("AWS_BUCKET")
    key = f"countries/countries_{timestamp}.json"
    s3.upload_file(filename, bucket, key)
    print(f"Uploaded to S3: {bucket}/{key}")

if __name__ == "__main__":
    data = extract_countries()
    filename, timestamp = save_locally(data)
    upload_to_s3(filename, timestamp)