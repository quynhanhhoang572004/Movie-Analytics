# 🎬 TMDB ETL Pipeline with PySpark & Looker Studio

This project builds a complete **ETL (Extract-Transform-Load) data pipeline** that fetches data from The Movie Database (TMDB) API, processes it with **PySpark**, and visualizes insights using **Looker Studio**. The pipeline leverages **Google Cloud Storage** and **BigQuery** for scalable storage and processing.

---

## 📌 Architecture Overview

![ETL Pipeline](./BigData%20architecture-Page-2.drawio.png)

---

## 🔧 Tech Stack

* **TMDB API** – Data source for movie information
* **Parquet Format** – Efficient columnar storage format
* **Google Cloud Storage (GCS)** – Stores intermediate parquet files
* **PySpark** – Distributed data processing and transformation
* **BigQuery** – Stores transformed data for querying
* **Looker Studio** – Visualizes insights from BigQuery

---

## 🚀 Pipeline Steps

### 1. **Extract: Fetching Data**

* Data is crawled from **TMDB API**
* The response is received in **JSON format**

### 2. **Transform: Format Conversion**

* JSON data is converted to **Parquet files**
* Parquet files are uploaded to **Google Cloud Storage**

### 3. **Load & Process: PySpark Transformation**

* A **Dataproc cluster** runs PySpark jobs
* PySpark reads Parquet files from GCS
* Applies data cleaning, transformation, and machine learning models (if applicable)

### 4. **Store: Load to BigQuery**

* Transformed data is written into **Google BigQuery**
* Ready for fast SQL queries and analytics

### 5. **Visualize: Looker Studio Dashboard**

* BigQuery data is connected to **Looker Studio**
* Dashboards are created to visualize KPIs and trends

---

## 🛠️ How to Run

### Prerequisites

* Python 3.8+
* Google Cloud SDK
* Access to:

  * TMDB API Key
  * Google Cloud Project with BigQuery, GCS, and Dataproc enabled

### 1. Set up environment

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Crawl and Convert JSON to Parquet

```bash
python fetch_and_convert.py
```

### 3. Upload to GCS

```bash
gsutil cp ./data/*.parquet gs://your-bucket-name/path/
```

### 4. Run PySpark Job

```bash
gcloud dataproc jobs submit pyspark transform.py \
  --cluster=your-dataproc-cluster \
  --region=your-region \
  --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar
```

### 5. Create Looker Dashboard

* Connect Looker Studio to your BigQuery dataset
* Design charts and dashboards using SQL queries

---

## 📁 Folder Structure



---

## 📊 Example Metrics

* Most popular movies by year
* Average rating per genre
* Trends in movie production over time

---

## 📄 License

This project is licensed under the MIT License.

---

Let me know if you'd like me to generate the actual code files (`fetch_and_convert.py`, `transform.py`, etc.) as well!
