import os
from google.cloud import dataproc_v1
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
REGION = os.getenv("REGION")
CLUSTER_NAME = os.getenv("CLUSTER_NAME")
GCS_PYSPARK_PATH = "gs://movie-data-bigdata/transform.py"
SERVICE_ACCOUNT_KEY = "keys/my_key.json"

job_client = dataproc_v1.JobControllerClient.from_service_account_file(
    SERVICE_ACCOUNT_KEY,
    client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
)

job_details = {
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": GCS_PYSPARK_PATH,
        "jar_file_uris": [
            "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.36.0.jar"
        ]
    }
}

job_request = dataproc_v1.SubmitJobRequest(
    project_id=PROJECT_ID,
    region=REGION,
    job=job_details
)

operation = job_client.submit_job_as_operation(request=job_request)
result = operation.result()
print(f"Job submitted successfully. Job ID: {result.reference.job_id}")