import time
import json
import uuid
import random
from pyspark.sql import SparkSession
from kafka import KafkaProducer
from schema import sensor_data_schema

spark = SparkSession.builder \
    .appName("DataGenerator") \
    .master("local[*]") \
    .getOrCreate()

cars_df = spark.read.parquet("s3a://spark/data/dims/cars")
# cars = cars_df.collect()

producer = KafkaProducer(
    bootstrap_servers=["course-kafka:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "sensors-sample"

while True:
    for car in cars_df.toLocalIterator():
        event = {
            "event_id": str(uuid.uuid4()),
            "event_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "car_id": car["car_id"],
            "speed": random.randint(0, 200),
            "rpm": random.randint(0, 8000),
            "gear": random.randint(1, 7)
        }

        producer.send(topic, value=event)

    producer.flush()

    time.sleep(1)

    # יצירת DataFrame עם ערכים אקראיים בהתאם לסכמה
cars_sensor_df = cars_df \
    .withColumn("event_id", F.expr("uuid()")) \
    .withColumn("event_time", F.current_timestamp()) \
    .withColumn("speed", (F.rand() * 201).cast("int")) \
    .withColumn("rpm", (F.rand() * 8001).cast("int")) \
    .withColumn("gear", (F.rand() * 7 + 1).cast("int")) \
    .select(*[f.name for f in sensor_data_schema.fields])

while True:
    for car in cars_sensor_df.toLocalIterator():
  
        event_json = {field.name: car[field.name] for field in sensor_data_schema.fields}
        producer.send(topic, value=event_json)