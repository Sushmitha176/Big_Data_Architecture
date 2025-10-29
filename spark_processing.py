from pyspark.sql import SparkSession
import pandas as pd

# Initialize Spark
spark = SparkSession.builder \
    .appName("EpidemicProcessing") \
    .getOrCreate()

def process_csv(file_path: str) -> pd.DataFrame:
    """Read CSV using Spark, do some transformations, return pandas DataFrame"""
    df_spark = spark.read.csv(file_path, header=True, inferSchema=True)
    
    # Example transformation: cast columns
    df_spark = df_spark.withColumn("new_cases", df_spark["new_cases"].cast("int")) \
                       .withColumn("deaths", df_spark["deaths"].cast("int")) \
                       .withColumn("recovered", df_spark["recovered"].cast("int"))
    
    # You can add more Big Data operations here (aggregations, filtering, etc.)
    return df_spark.toPandas()
