import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

class MovieAnalytics:
    spark = None

    def __init__(self):
        os.environ['HADOOP_HOME'] = "/opt/hadoop"  
        
        if MovieAnalytics.spark is None:
            print("Creating Spark Session")
            self.spark = SparkSession.builder.appName("MovieAnalytics").getOrCreate()

        df_action = self.spark.read.parquet("data_parquet/movies_action.parquet").withColumn("genre", lit("Action"))
        df_adventure = self.spark.read.parquet("data_parquet/movies_adventure.parquet").withColumn("genre", lit("Adventure"))
        df_animation = self.spark.read.parquet("data_parquet/movies_animation.parquet").withColumn("genre", lit("Animation"))
        df_comedy = self.spark.read.parquet("data_parquet/movies_comedy.parquet").withColumn("genre", lit("Comedy"))
        df_crime = self.spark.read.parquet("data_parquet/movies_crime.parquet").withColumn("genre", lit("Crime"))
        
        df = df_action.union(df_adventure).union(df_comedy).union(df_animation).union(df_crime)

        self.df_unique = df.dropDuplicates(["title"])


    #Top X most popular movies
    def get_top_popular_movies(self, limit):
        top10 = self.df_unique.select("title", "popularity") \
                .orderBy(col("popularity").desc()) \
                .limit(limit)
        top10.show(truncate=False)
        return top10

    #Genres ranked from most to least popular
    def rank_genres_by_popularity(self):
        genre_popularity = self.df_unique.groupBy("genre").sum("popularity").withColumnRenamed("sum(popularity)", "total_popularity")
        top_genres = genre_popularity.orderBy(col("total_popularity").desc())
        top_genres.show(truncate=False)
        return top_genres
    
    #Genres ranked from highest to lowest average rating
    def rank_genres_by_vote_average(self):
        genre_vote = self.df_unique.groupBy("genre").sum("vote_average").withColumnRenamed("sum(vote_average)", "total_average")
        top_genres =  genre_vote.orderBy(col("total_average").desc())
        top_genres.show(truncate=False)
        return top_genres

    #Top X recent movies
    def top_recent_movies(self, limit):
        top_recent = self.df_unique.select("title", "release_date") \
                    .orderBy(col("release_date").desc()) \
                    .limit(limit)
        top_recent.show(truncate=False)
        return top_recent

    #Top X highest rated movies
    def top_highest_rated_movies(self, limit):
        top_rated = self.df_unique.select("title", "vote_average") \
                    .orderBy(col("vote_average").desc()) \
                    .limit(limit)
        top_rated.show(truncate=False)
        return top_rated

    
   
