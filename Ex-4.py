import time
import json
import uuid
import random
from pyspark.sql import SparkSession
from kafka import KafkaProducer
from pyspark.sql import functions as F
from schema import sensor_data_schema

spark = SparkSession.builder \
    .appName("DataGenerator") \
    .master("local[*]") \
    .getOrCreate()

cars_df = spark.read.parquet("s3a://spark/data/dims/cars")

cars_sensor_df = cars_df \
    .withColumn("event_id", F.expr("uuid()")) \
    .withColumn("event_time", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("speed", (F.rand() * 201).cast("int")) \
    .withColumn("rpm", (F.rand() * 8001).cast("int")) \
    .withColumn("gear", (F.rand() * 7 + 1).cast("int")) \
    .select(*[f.name for f in sensor_data_schema.fields])

producer = KafkaProducer(
    bootstrap_servers=["course-kafka:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "sensors-sample"

while True:
    for car in cars_sensor_df.toLocalIterator():
      
        event_json = car.asDict()
        producer.send(topic, value=event_json)

    producer.flush()
    time.sleep(1)



