from minio import Minio
from src.config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    BRONZE_BUCKET,
    SILVER_BUCKET,
    GOLD_BUCKET,
)

def create_bucket(client: Minio, bucket_name: str) -> None:
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' created successfully.")
    else:
        print(f"Bucket '{bucket_name}' already exists.")

def main():

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
    
    create_bucket(client, BRONZE_BUCKET)
    create_bucket(client, SILVER_BUCKET)
    create_bucket(client, GOLD_BUCKET)

if __name__ == "__main__":
    main()
