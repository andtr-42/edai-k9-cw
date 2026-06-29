from minio import Minio
from src.config import (
    LAKEHOUSE_ENDPOINT,
    LAKEHOUSE_ACCESS_KEY,
    LAKEHOUSE_SECRET_KEY,
    BRONZE_BUCKET,
    SILVER_BUCKET,
)

def create_bucket(client: Minio, bucket_name: str) -> None:
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' created successfully.")
    else:
        print(f"Bucket '{bucket_name}' already exists.")

def main():

    client = Minio(
        LAKEHOUSE_ENDPOINT,
        access_key=LAKEHOUSE_ACCESS_KEY,
        secret_key=LAKEHOUSE_SECRET_KEY,
        secure=False,
    )
    
    create_bucket(client, BRONZE_BUCKET)
    create_bucket(client, SILVER_BUCKET)

if __name__ == "__main__":
    main()
