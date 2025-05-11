from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *


class TMDBMovieProcessor:
    def __init__(self, spark: SparkSession, input_dir: str, dataset: str, project_id: str):
        self.spark = spark
        self.input_dir = input_dir.rstrip("/")
        self.dataset = dataset
        self.project_id = project_id

    def process_all_files(self, file_map: dict):
        for filename, table_suffix in file_map.items():
            print(f"▶ Loading raw file: {filename}")
            try:
                df = self._read_parquet(f"{self.input_dir}/{filename}")
                print(f"Row count: {df.count()}")  
                self._write_to_bigquery(df, table_suffix)
                print(f"Raw data written to BigQuery: {table_suffix}")
            except Exception as e:
                print(f"Failed loading {filename}: {e}")

    def _read_parquet(self, path: str) -> DataFrame:
        return self.spark.read.parquet(path)

    def _write_to_bigquery(self, df: DataFrame, table_suffix: str):
        table_path = f"{self.project_id}.{self.dataset}.{table_suffix}"
        df.write.format("bigquery") \
            .option("table", table_path) \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()
        
if __name__ == "__main__":
    spark = SparkSession.builder.appName("TMDBMovieParquetPipeline").getOrCreate()

    file_to_table_map = {
       "movies_adventure.parquet": "movies_adventure",
        "movies_crime.parquet": "movies_crime",
        "movies_family.parquet": "movies_family",

      
    }

    processor = TMDBMovieProcessor(
        spark=spark,
        input_dir="gs://movie-data-bigdata/data/movies",
        dataset="tmdb_dataset",
        project_id="big-data-project-459118"
    )
    processor.process_all_files(file_to_table_map)
