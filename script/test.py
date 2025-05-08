from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "text"])
print(df.show())