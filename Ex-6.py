
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from schema import enriched_data_schema


spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('AlertDetection') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()



stream_df = spark\
    .readStream\
    .format("kafka")\
    .option("startingOffsets", "earliest")\
    .option("kafka.bootstrap.servers", 'course-kafka:9092')\
    .option("subscribe", "samples-enriched")\
    .load()\
    .select (F.col("value").cast("string"))

parse_df=stream_df\
         .select(F.from_json(F.col("value"),enriched_data_schema).alias("value"))\
         .select("value.*")

alerts_df = parse_df.filter(
    (F.col("speed") > 120) |
    (F.col("expected_gear") != F.col("gear")) |
    (F.col("rpm") > 6000)
)

alerts_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("topic", "alert-data") \
    .option("checkpointLocation", "s3a://spark/checkpoints/alerts") \
    .start() \
    .awaitTermination()

# alerts_df.writeStream \
#     .format("console") \
#     .option("truncate", False) \
#     .outputMode("append") \
#     .start() \
#     .awaitTermination()
