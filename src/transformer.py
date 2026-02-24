from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def transform_data(spark, file_path):

    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("transaction_time", TimestampType(), False),
        StructField("amount", DoubleType(), False),
        StructField("status", StringType(), True)
    ])

    df = spark.read.csv(file_path, header=True, schema=schema)

    # Validation
    valid_df = df.filter(
        (col("transaction_id").isNotNull()) &
        (col("user_id").isNotNull()) &
        (col("transaction_time").isNotNull()) &
        (col("amount") > 0)
    )

    # Deduplication
    window_spec = Window.partitionBy("transaction_id") \
                        .orderBy(col("transaction_time").desc())

    dedup_df = valid_df.withColumn(
                    "row_num",
                    row_number().over(window_spec)
                ) \
                .filter(col("row_num") == 1) \
                .drop("row_num")

    return dedup_df