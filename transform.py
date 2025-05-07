from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, explode
from pyspark.sql.types import *


class TMDBMovieProcessor:
    def __init__(self, spark: SparkSession, input_dir: str, dataset: str, project_id: str):
        self.spark = spark
        self.input_dir = input_dir.rstrip("/")
        self.dataset = dataset
        self.project_id = project_id
        self.schema = self._define_schema()

    def _define_schema(self) -> StructType:
        return StructType([
            StructField("id", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("original_title", StringType(), True),
            StructField("original_language", StringType(), True),
            StructField("release_date", StringType(), True),
            StructField("popularity", DoubleType(), True),
            StructField("vote_average", DoubleType(), True),
            StructField("vote_count", IntegerType(), True),
            StructField("overview", StringType(), True),
            StructField("genre_ids", ArrayType(IntegerType()), True),
            StructField("adult", BooleanType(), True),
            StructField("backdrop_path", StringType(), True),
            StructField("poster_path", StringType(), True),
            StructField("video", BooleanType(), True)
        ])


    def process_all_files(self, file_map: dict):
        for filename, table_suffix in file_map.items():
            print(f"▶ Processing file: {filename}")
            try:
                df = self._read_and_parse_json(f"{self.input_dir}/{filename}")
                df_clean = df.dropDuplicates(["id"])
                self._write_to_bigquery(df_clean, table_suffix)
                print(f"✅ Written to BigQuery table: {table_suffix}")
            except Exception as e:
                print(f"❌ Failed processing {filename}: {e}")

    def _read_and_parse_json(self, path: str) -> DataFrame:
        raw_df = self.spark.read.text(path)
        json_df = raw_df.select(from_json("value", ArrayType(self.schema)).alias("data"))
        return json_df.select(explode("data").alias("movie")).select("movie.*")

    def _write_to_bigquery(self, df: DataFrame, table_suffix: str):
        table_path = f"{self.project_id}.{self.dataset}.{table_suffix}"
        df.write.format("bigquery") \
            .option("table", table_path) \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("TMDBMoviePipeline").getOrCreate()

    file_to_table_map = {
        "movies_action.json": "movies_action",
        "movies_comedy.json": "movies_comedy",
        "movies_drama.json": "movies_drama",
        # Add more as needed
    }

    processor = TMDBMovieProcessor(
        spark=spark,
        input_dir="gs://movie-data-bigdata/data/movies",
        dataset="tmdb_dataset",
        project_id="big-data-project-459118"
    )
    processor.process_all_files(file_to_table_map)
