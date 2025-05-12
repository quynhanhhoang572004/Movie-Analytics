import json
import io
import time
import pandas as pd
from google.cloud import storage

class GCSUploader:
    def __init__(self, bucket_name, key_path):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client.from_service_account_json(key_path)
        self.bucket = self.storage_client.bucket(bucket_name)
    
    def upload_parquet(self, data: str | list | dict, destination_blob_name: str, max_retries=3):
        attempt = 1
        while attempt <= max_retries:
            try:
                if isinstance(data, str):
                    data = json.loads(data)

                df = pd.DataFrame(data if isinstance(data, list) else [data])
                
                buffer = io.BytesIO()
                df.to_parquet(buffer, index=False)
                buffer.seek(0)

                blob = self.bucket.blob(destination_blob_name)
                blob.upload_from_file(buffer, content_type="application/octet-stream")

                if not blob.exists(self.storage_client):
                    raise RuntimeError("Upload verification failed")
                
                print(f"Uploaded Parquet: gs://{self.bucket_name}/{destination_blob_name}")
                return True
            except Exception as e:
                print(f"Retry {attempt} for {destination_blob_name} due to error: {e}")
                time.sleep(3)
                attempt += 1

        print(f"Failed to upload {destination_blob_name} after {max_retries} attempts.")
        return False
