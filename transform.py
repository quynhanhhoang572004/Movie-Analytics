from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

class MovieTransformer:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = input_path
        self.output_path = output_path
        self.spark = SparkSession.builder.appName("TMDbMoviePipeline").getOrCreate()

    def read_data(self) -> DataFrame:
        print(f"Reading data from: {self.input_path}")
        df = self.spark.read.json(self.input_path)
        print(f"Loaded {df.count()} records.")
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        print("Transforming data...")
        return df.select(
            "id", "title", "original_language", "release_date",
            "popularity", "vote_average", "vote_count",
            "overview", "budget", "revenue", "runtime"
        ).dropna()

    def write_data(self, df: DataFrame):
        print("Writing cleaned data to BigQuery...")
        df.write.format("bigquery") \
            .option("table", "big-data-project-459105.tmdb_dataset.movie_dataset2025") \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()
        print("Write to BigQuery complete.")

    def run(self):
        df = self.read_data()
        df_clean = self.transform(df)
        self.write_data(df_clean)


if __name__ == "__main__":
    input_path = "gs://movie-data-bigdata/data/movies/*.json"
    output_path = "gs://movie-data-bigdata/data_parquet/movies_cleaned"

    pipeline = MovieTransformer(input_path, output_path)
    pipeline.run()
