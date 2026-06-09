from minio import Minio

BRONZE_BUCKET_NAME = "bronze-bucket"
SILVER_BUCKET_NAME = "silver-bucket"

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)


def create_bronze_bucket():
    if not client.bucket_exists(BRONZE_BUCKET_NAME):
        client.make_bucket(BRONZE_BUCKET_NAME)
        print(f"Bucket '{BRONZE_BUCKET_NAME}' created successfully.")
    else:
        print(f"Bucket '{BRONZE_BUCKET_NAME}' already exists.")


def create_silver_bucket():
    if not client.bucket_exists(SILVER_BUCKET_NAME):
        client.make_bucket(SILVER_BUCKET_NAME)
        print(f"Bucket '{SILVER_BUCKET_NAME}' created successfully.")
    else:
        print(f"Bucket '{SILVER_BUCKET_NAME}' already exists.")


def main():
    create_bronze_bucket()
    create_silver_bucket()


if __name__ == "__main__":
    main()
