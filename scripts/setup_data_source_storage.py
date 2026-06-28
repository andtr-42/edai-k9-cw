from minio import Minio
from src.config import (
    DATA_SOURCE_ENDPOINT,
    DATA_SOURCE_ACCESS_KEY,
    DATA_SOURCE_SECRET_KEY,
    DATA_SOURCE_BUCKET,
)

def main():
    print(f"Connecting to Data Source: {DATA_SOURCE_ENDPOINT}")
    
    client = Minio(
        DATA_SOURCE_ENDPOINT,
        access_key=DATA_SOURCE_ACCESS_KEY,
        secret_key=DATA_SOURCE_SECRET_KEY,
        secure=False,
    )
    
    if not client.bucket_exists(DATA_SOURCE_BUCKET):
        client.make_bucket(DATA_SOURCE_BUCKET)
        print(f"✔ Created target bucket: {DATA_SOURCE_BUCKET}")
    else:
        print(f"ℹ Bucket already exists: {DATA_SOURCE_BUCKET}")

if __name__ == "__main__":
    main()