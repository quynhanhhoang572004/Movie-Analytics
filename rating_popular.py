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


df_action = spark.read.parquet('/app/data_parquet/ratings_action.parquet')
df_adventure = spark.read.parquet('/app/data_parquet/ratings_adventure.parquet')
df_animation = spark.read.parquet('/app/data_parquet/ratings_animation.parquet')
df_comedy = spark.read.parquet('/app/data_parquet/ratings_comedy.parquet')
df_crime = spark.read.parquet('/app/data_parquet/ratings_crime.parquet')

window_spec = Window.orderBy(desc("popularity"))

def get_top_movies_by_genre(df, top_n: int = 5):
    window_spec = Window.orderBy(desc("popularity"))
    result = (
        df.select("title", "vote_average", "vote_count", "popularity")
          .withColumn("rank", row_number().over(window_spec))
          .filter(col("rank") <= top_n)
          .select("rank", "title", "vote_average", "vote_count", "popularity")
          
    )
    return result
  
def get_top_movies_by_voteAvg(df, top_n: int = 5):
    window_spec = Window.orderBy(desc("vote_average"))
    result = (
        df.select("title", "vote_average", "vote_count", "popularity")
          .withColumn("rank", row_number().over(window_spec))
          .filter(col("rank") <= top_n)
          .select("rank", "title", "vote_average", "vote_count", "popularity")
          
    )
    return result
  


#top_action = get_top_movies_by_genre(df_action)
#top_adventure = get_top_movies_by_genre(df_adventure)
#top_animation = get_top_movies_by_genre(df_animation)
#top_comedy = get_top_movies_by_genre(df_comedy)
#top_crime = get_top_movies_by_genre(df_crime)

topAvg_action = get_top_movies_by_voteAvg(df_action);
topAvg_adventure = get_top_movies_by_voteAvg(df_adventure)
topAvg_animation = get_top_movies_by_voteAvg(df_animation)
topAvg_comedy = get_top_movies_by_voteAvg(df_comedy)
topAvg_crime = get_top_movies_by_voteAvg(df_comedy)

topAvg_action.show(truncate=False)
topAvg_adventure.show(truncate=False)
topAvg_animation.show(truncate=False)
topAvg_comedy.show(truncate=False)
topAvg_crime.show(truncate=False)