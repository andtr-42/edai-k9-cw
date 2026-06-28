import os
from pathlib import Path
from minio import Minio
from src.config import (
    DATA_SOURCE_ENDPOINT,
    DATA_SOURCE_ACCESS_KEY,
    DATA_SOURCE_SECRET_KEY,
    DATA_SOURCE_BUCKET,
    OFFLINE_DATA_PATH,  # Assuming this is defined in your config
)

def ingest_local_data(client: Minio, local_base_path: Path, bucket_name: str):
    """Recursively finds all files in the local directory and uploads them to MinIO."""
    if not local_base_path.exists():
        print(f"❌ Local path {local_base_path} does not exist. Run data generation first.")
        return

    print(f"🚀 Starting ingestion from {local_base_path} to bucket '{bucket_name}'...")
    
    # Recursively find all files (e.g., .parquet)
    for file_path in local_base_path.rglob("*"):
        if file_path.is_file():
            # Create the object key relative to the base output directory
            # This preserves folders like movies/, playbacks/playback_date=.../
            object_name = str(file_path.relative_to(local_base_path)).replace("\\", "/")
            
            print(f"Uploading: {object_name}...")
            client.fput_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=str(file_path),
            )
            
    print("✔ Ingestion complete!")

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

    # Sync local directory structure to MinIO
    ingest_local_data(
        client=client, 
        local_base_path=Path(OFFLINE_DATA_PATH), 
        bucket_name=DATA_SOURCE_BUCKET
    )

if __name__ == "__main__":
    main()