import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, row_number, lit
from pyspark.sql.window import Window



os.environ['HADOOP_HOME'] = "/opt/hadoop"

spark = SparkSession.builder \
    .master('local') \
    .appName('Rating') \
    .config('spark.executor.memory', '5gb') \
    .config("spark.cores.max", "6") \
    .getOrCreate()


df_action = spark.read.parquet('/app/data/movies_action.parquet')
df_action.select("id", "title").limit(50).show(50, truncate=False)

