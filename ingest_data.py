import os
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()
input_dir = "data"
output_dir = "data_parquet"


key_path = "keys/mykey.json"
bucket_name = os.getenv("BUCKET_NAME")
destination_folder = "data_parquet"

class IngestData:
    def __init__(self, data_source, cloud_bucket_name, cloud_key_path):
        self.data_source = data_source
        self.cloud_bucket_name = cloud_bucket_name
        self.cloud_key_path = cloud_key_path

    def load_data(self):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        for filename in os.listdir(self.data_source):
            if filename.endswith(".csv"):
                file_path = os.path.join(self.data_source, filename)
                
                df = pd.read_csv(file_path)
                
                parquet_filename = filename.replace(".csv", ".parquet")
                parquet_path = os.path.join(output_dir, parquet_filename)
                
             
                df.to_parquet(parquet_path, index=False)
                print(f"Saved {parquet_filename}")

    def upload_to_gcp(self):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.cloud_key_path

        
            client = storage.Client()
            bucket = client.get_bucket(self.cloud_bucket_name)

            for filename in os.listdir(output_dir):
                if filename.endswith(".parquet"):
                    local_path = os.path.join(output_dir, filename)
                    blob_path = os.path.join(destination_folder, filename)

                    blob = bucket.blob(blob_path)
                    blob.upload_from_filename(local_path)

                    print(f"Uploaded {filename} to gs://{self.cloud_bucket_name}/{blob_path}")


        


if __name__ == "__main__":
    ingester = IngestData(input_dir,bucket_name,key_path)
    ingester.load_data()
    ingester.upload_to_gcp()
