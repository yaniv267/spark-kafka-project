from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import Row
from schema import car_colors_schema

spark = SparkSession.builder \
    .appName("ModelCreation") \
    .master("local[*]") \
    .getOrCreate()

print(spark.version)

car_colors = [
    Row(color_id=1, color_name="Black"),
    Row(color_id=2, color_name="Red"),
    Row(color_id=3, color_name="Gray"),
    Row(color_id=4, color_name="White"),
    Row(color_id=5, color_name="Green"),
    Row(color_id=6, color_name="Blue"),
    Row(color_id=7, color_name="Pink")
]


models_df = spark.createDataFrame(car_colors,schema=car_colors_schema)

models_df.write.mode("overwrite").parquet("s3a://spark/data/dims/car_colors")


spark.stop()
