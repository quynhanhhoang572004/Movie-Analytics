import os
os.environ['HADOOP_HOME'] = "/opt/hadoop"  

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet('/app/data_parquet/genres.parquet')
print(df.show())
