
from pyspark.sql import types as T


car_models_schema = T.StructType([
    T.StructField("model_id", T.IntegerType()),
    T.StructField("car_brand", T.StringType()),
    T.StructField("car_model", T.StringType())
])


car_colors_schema = T.StructType([
    T.StructField("color_id", T.IntegerType()),
    T.StructField("color_name", T.StringType())
])


cars_schema = T.StructType([
    T.StructField("car_id", T.StringType()),
    T.StructField("driver_id", T.StringType()),
    T.StructField("model_id", T.IntegerType()),
    T.StructField("color_id", T.IntegerType())
])


sensor_data_schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_time", T.TimestampType()),
    T.StructField("car_id", T.StringType()),
    T.StructField("speed", T.IntegerType()),
    T.StructField("rpm", T.IntegerType()),
    T.StructField("gear", T.IntegerType())
])

enriched_data_schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_time", T.TimestampType()),
    T.StructField("car_id", T.StringType()),
    T.StructField("speed", T.IntegerType()),
    T.StructField("rpm", T.IntegerType()),
    T.StructField("gear", T.IntegerType()),
    T.StructField("driver_id", T.StringType()),   
    T.StructField("brand_name", T.StringType()),     
    T.StructField("model_name", T.StringType()),  
    T.StructField("color_name", T.StringType()),     
    T.StructField("expected_gear", T.DoubleType())
])


sensor_data_schema_data = T.StructType([
    T.StructField("event_id", T.StringType(), True),
    T.StructField("event_time", T.StringType(), True),  # ← String! תואם JSON
    T.StructField("car_id", T.IntegerType(), True),
    T.StructField("speed", T.IntegerType(), True),
    T.StructField("rpm", T.IntegerType(), True),
    T.StructField("gear", T.IntegerType(), True)
])