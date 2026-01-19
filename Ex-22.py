import time
import json
import uuid
import random
from pyspark.sql import SparkSession
from kafka import KafkaProducer


spark = SparkSession.builder \
    .appName("DataGenerator") \
    .getOrCreate()

cars_df = spark.read.parquet("s3a://spark/data/dims/cars")
cars = cars_df.collect()

producer = KafkaProducer(
    bootstrap_servers=["course-kafka:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "sensors-sample"

while True:
    for car in cars:
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