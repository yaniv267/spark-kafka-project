from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import types as T
from schema import cars_schema

spark=SparkSession.builder\
    .appName('CarsGenerator') \
    .master("local[*]") \
    .getOrCreate()

cars_num=20

cars_df = spark.range(cars_num) \
    .withColumn("car_id", (F.rand() * 9000000 + 1000000).cast("int"))\
    .withColumn("driver_id", (F.rand() * 900000000 + 100000000).cast("int"))\
    .withColumn("model_id", (F.rand() * 7 + 1).cast("int"))\
    .withColumn("color_id", (F.rand() * 7 + 1).cast("int"))\
    .select("car_id", "driver_id", "model_id", "color_id")


cars_df.select(
    F.min("car_id").alias("min_car_id"),
    F.max("car_id").alias("max_car_id"),
    F.min("driver_id").alias("min_driver_id"),
    F.max("driver_id").alias("max_driver_id"),
    F.min("model_id").alias("min_model_id"),
    F.max("model_id").alias("max_model_id"),
    F.min("color_id").alias("min_color_id"),
    F.max("color_id").alias("max_color_id")
).show()


cars_df.write.mode("overwrite").parquet("s3a://spark/data/dims/cars")



spark.stop()
