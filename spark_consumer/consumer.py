import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_csv, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

class Consumer:
    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)

        self.topic = self.config["kafka"]["topic"]
        self.bootstrap_servers = self.config["kafka"]["bootstrap_servers"]
        self.output_path = self.config["parquet"]["output_path"]
        self.checkpoint_path = self.config["parquet"]["checkpoint_path"]

        self.spark = SparkSession.builder \
            .appName(self.config["spark"]["app_name"]) \
            .getOrCreate()

        self.schema = self._get_schema()
    
    def _load_config(self, path: str) -> dict:
        with open(path, "r") as f:
            return yaml.safe_load(f)

    def _get_schema(self) -> StructType:
        return StructType([
            StructField("genre", StringType()),
            StructField("id", StringType()),
            StructField("title", StringType()),
            StructField("original_title", StringType()),
            StructField("original_language", StringType()),
            StructField("release_date", StringType()),
            StructField("popularity", DoubleType()),
            StructField("vote_average", DoubleType()),
            StructField("vote_count", IntegerType()),
            StructField("adult", StringType()),
            StructField("video", StringType()),
            StructField("overview", StringType()),
            StructField("budget", IntegerType()),
            StructField("revenue", IntegerType()),
            StructField("runtime", DoubleType()),
            StructField("status", StringType()),
            StructField("homepage", StringType()),
            StructField("genres", StringType()),
            StructField("production_companies", StringType()),
            StructField("production_countries", StringType())
        ])
