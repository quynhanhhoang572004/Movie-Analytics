

```markdown
# Deploy Dataproc Cluster and Submit PySpark Job on Google Cloud (Local Setup)

## 📦 Requirements
- Google Cloud account (Billing enabled)
- Terraform installed ([Install Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli))
- Google Cloud SDK (gcloud) installed ([Install gcloud](https://cloud.google.com/sdk/docs/install))
- Python (optional, for writing PySpark scripts)

---

## ⚙️ Setup and Authentication

### 1. Authenticate gcloud
```bash
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your-key.json" 
```
✅ This will save your credentials locally for Terraform to use.

File json mình sẽ gửi sau 

---

## 🛠 Deploy Infrastructure with Terraform

### 2. Clone the project
```bash
git clone <your-github-repo-or-folder>
cd <your-project-folder>
```

### 3. Initialize Terraform
```bash
terraform init
```

### 4. Apply Terraform to create Bucket and Dataproc Cluster
```bash
terraform apply
```
✅ This will create:
- A Google Cloud Storage Bucket
- A Dataproc Spark Cluster ready for PySpark jobs

---

## ☁️ Upload PySpark Script to GCS

### 5. Prepare your PySpark script
Example (`wordcount.py`):
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('WordCount').getOrCreate()

data = ["hello world", "hello from gcp", "hello from pyspark"]
rdd = spark.sparkContext.parallelize(data)

words = rdd.flatMap(lambda line: line.split(" "))
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

for word, count in wordCounts.collect():
    print(f"{word}: {count}")

spark.stop()
```

Save it as `wordcount.py`.

### 6. Upload the script to GCS
```bash
gsutil cp wordcount.py gs://<your-bucket-name>/
```

---

## 🚀 Submit PySpark Job to Dataproc Cluster

### 7. Submit the job
```bash
gcloud dataproc jobs submit pyspark gs://<your-bucket-name>/wordcount.py \
    --cluster=pyspark-cluster \
    --region=us-central1
```

✅ You will see the output printed in the terminal.

---

## 📚 How to Read and Write Data on GCS in PySpark

**Reading CSV from GCS:**
```python
df = spark.read.csv("gs://<your-bucket-name>/data/myfile.csv", header=True, inferSchema=True)
df.show()
```

**Reading Parquet from GCS:**
```python
df = spark.read.parquet("gs://<your-bucket-name>/data/myfile.parquet")
df.show()
```

**Saving DataFrame to GCS as Parquet:**
```python
df.write.parquet("gs://<your-bucket-name>/output/myoutput.parquet")
```

✅ No download needed — Spark works directly with GCS.

---

## 🧹 Clean Up (Important to avoid billing)

### 8. Delete Dataproc Cluster
```bash
gcloud dataproc clusters delete pyspark-cluster --region=us-central1
```

### 9. Delete the GCS Bucket (optional)
```bash
gsutil rm -r gs://<your-bucket-name>
```

---

# 🎯 Summary

| Step | Command |
|:-----|:--------|
| Authenticate | `gcloud auth application-default login` |
| Terraform Init | `terraform init` |
| Terraform Apply | `terraform apply` |
| Upload script | `gsutil cp script.py gs://bucket/` |
| Submit job | `gcloud dataproc jobs submit pyspark ...` |
| Clean up | `gcloud dataproc clusters delete ...` |

---

# ⚡ Notes
- Always check Google Cloud billing dashboard.
- Stop cluster when you finish to avoid charges.
- Storage costs are small, cluster compute costs are higher.

---


## 1. Build the docker image
docker build -t pyspark-docker .

## 2. Prepare your pyspark script

## 3. Run the script in docker
docker run -it --rm -p 4041:4040 -v ${PWD}/test.py:/app/test.py pyspark-docker python /app/test.py (your file python)

```bash