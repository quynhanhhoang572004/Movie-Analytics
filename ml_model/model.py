from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import when
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import year, month, to_date, col
from pyspark.ml.feature import StandardScaler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator



class LoadData:
    def __init__(self, spark, project_id, table_name):
        self.spark = spark
        self.project_id = project_id
        self.table_name = table_name

    def load(self):
        table_path = f"{self.project_id}.{self.table_name}"
        print(f"Loading data from {table_path}")
        return self.spark.read.format("bigquery") \
            .option("table", table_path) \
            .load()



class Preprocess:
    def __init__(self):
         self.feature_cols = ["popularity", "vote_average", "release_year", "release_month"]


    def transform(self, df):
        print("Preprocessing")
        df = df.withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd"))
        df = df.withColumn("release_year", year(col("release_date")))
        df = df.withColumn("release_month", month(col("release_date")))
        df = df.withColumn("label",
                when(df["vote_average"] < 6.5, 0).otherwise(1))
        df = df.dropna(subset=["popularity", "vote_average", "release_year", "release_month"])

        assembler = VectorAssembler(inputCols=self.feature_cols, outputCol="assembler_features")
        df = assembler.transform(df)
        scaler = StandardScaler(inputCol="assembler_features", outputCol="scaled_features", withMean=True, withStd=True)
        scaler_model = scaler.fit(df)
        df = scaler_model.transform(df)

        return df.select("scaled_features", "label", "id", "title", "vote_average")



class TrainModel:
    def __init__(self):
        print("Starting training model")

    def train(self, df):
        print("Training")
        lr = LogisticRegression(featuresCol="scaled_features", labelCol="label", family="binomial")

        paramGrid = ParamGridBuilder() \
            .addGrid(lr.regParam, [0.01, 0.1, 1.0]) \
            .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
            .build()
        
        evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")

        crossval = CrossValidator(
        estimator=lr,
        estimatorParamMaps=paramGrid,
        evaluator=evaluator,
        numFolds=3, 
        parallelism=2  
    )
        cv_model = crossval.fit(df)

        return cv_model.bestModel


class ModelEvaluator:
    def evaluate(self, model, df, genre):
        print("Evaluate ML learning model")
        predictions = model.transform(df)

        
        evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
        auc = evaluator.evaluate(predictions)

        
        acc_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
        accuracy = acc_evaluator.evaluate(predictions)

    
        confusion_df = predictions.groupBy("label", "prediction").count().orderBy("label", "prediction")
        print(f"\nConfusion Matrix for {genre}:")
        confusion_df.show()

        print(f"AUC: {auc:.4f} | Accuracy: {accuracy:.4f}")

        return predictions


class Predict:
    def __init__(self, project_id, output_table):
        self.project_id = project_id
        self.output_table = output_table

    def save(self, predictions):
        print(f"Saving predict result: {self.output_table}")
        predictions.select("id", "title", "vote_average", "probability", "prediction") \
            .write.format("bigquery") \
            .option("table", f"{self.project_id}.{self.output_table}") \
            .mode("overwrite") \
            .save()


class Pipeline:
    def __init__(self, spark, genre, input_table, output_table, project_id):
        self.spark = spark
        self.genre = genre
        self.project_id = project_id
        self.data_loader = LoadData(spark, project_id, input_table)
        self.preprocessor = Preprocess()
        self.trainer = TrainModel()
        self.evaluator = ModelEvaluator()
        self.saver = Predict(project_id, output_table)

    def run(self):
        df = self.data_loader.load()
        df_prepared = self.preprocessor.transform(df)
        model = self.trainer.train(df_prepared)
        predictions = self.evaluator.evaluate(model, df_prepared, self.genre)
        self.saver.save(predictions)



if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("MLmodel") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.0") \
        .config("temporaryGcsBucket", "movie-data-bigdata-1") \
        .getOrCreate()


    project_id = "big-data-project-459118"
    genres = ["crime", "comedy", "drama", "family", "action"]

    for genre in genres:
        input_table = f"tmdb_dataset.movies_{genre}"
        output_table = f"tmdb_dataset.logistic_regression_predictions_{genre}"

        pipeline = Pipeline(
            spark=spark,
            genre=genre,
            input_table=input_table,
            output_table=output_table,
            project_id=project_id
        )
        pipeline.run()

    spark.stop()
