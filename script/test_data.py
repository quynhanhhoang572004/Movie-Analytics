from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("InspectSchema").getOrCreate()

df = spark.read.parquet("data/data_movies_movies_action.parquet")
df.printSchema()
df.show(5, truncate=False)