from pyspark.sql import SparkSession, functions as F
from schema import sensor_data_schema_data

spark = SparkSession.builder \
    .appName('kafka-validate') \
    .master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()

# -----------------------------
# קריאה מ-Kafka
# -----------------------------
stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("subscribe", "sensors-sample") \
    .option("startingOffsets", "earliest") \
    .load()

# -----------------------------
# פענוח JSON
# -----------------------------
parsed = stream_df.select(
    F.from_json(F.col("value").cast("string"), sensor_data_schema_data).alias("v")
).select("v.*")  # חשוב! עכשיו כל שדה JSON נמצא כעמודה רגילה

# המר event_time ל-Timestamp של Spark
parsed = parsed.withColumn(
    "event_time",
    F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss")
)

# -----------------------------
# הצגת הנתונים בקונסול
# -----------------------------
# parsed.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .option("truncate", False) \
#     .start() \
#     .awaitTermination()

stats = parsed.select(
    F.min("speed").alias("min_speed"),
    F.max("speed").alias("max_speed"),
    F.min("rpm").alias("min_rpm"),
    F.max("rpm").alias("max_rpm"),
    F.min("gear").alias("min_gear"),
    F.max("gear").alias("max_gear")
)

query = stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()\
    .awaitTermination()

