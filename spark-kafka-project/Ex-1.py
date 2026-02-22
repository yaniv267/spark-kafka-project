from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import Row
from schema import car_models_schema

spark = SparkSession.builder \
    .appName("ModelCreation") \
    .master("local[*]") \
    .getOrCreate()

Car_Models = [
    Row(model_id=1, car_brand="Mazda", car_model="3"),
    Row(model_id=2, car_brand="Mazda", car_model="6"),
    Row(model_id=3, car_brand="Toyota", car_model="Corolla"),
    Row(model_id=4, car_brand="Hyundai", car_model="i20"),
    Row(model_id=5, car_brand="Kia", car_model="Sportage"),
    Row(model_id=6, car_brand="Kia", car_model="Rio"),
    Row(model_id=7, car_brand="Kia", car_model="Picanto")
]



models_df = spark.createDataFrame(Car_Models,schema=car_models_schema)

models_df.write.mode("overwrite").parquet("s3a://spark/data/dims/car_models")


spark.stop()