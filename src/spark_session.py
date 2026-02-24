from pyspark.sql import SparkSession

def create_spark():
    spark = SparkSession.builder \
        .appName("IncrementalDataPlatform") \
        .getOrCreate()
    return spark