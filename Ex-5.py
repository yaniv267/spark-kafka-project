from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import types as T
from schema import sensor_data_schema


spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('data_Enrichment') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()

models_df = spark.read.parquet("s3a://spark/data/dims/car_models") 
colors_df = spark.read.parquet("s3a://spark/data/dims/car_colors")
cars_df = spark.read.parquet("s3a://spark/data/dims/cars")


stream_df = spark\
    .readStream\
    .format("kafka")\
    .option("startingOffsets", "earliest")\
    .option("kafka.bootstrap.servers", 'course-kafka:9092')\
    .option("subscribe", "sensors-sample")\
    .load()\
    .select (F.col("value").cast("string"))

parse_df=stream_df\
         .select(F.from_json(F.col("value"),sensor_data_schema).alias("value"))\
         .select("value.*")
    


enriched_df = parse_df \
    .join(cars_df, "car_id") \
    .join(models_df, "model_id") \
    .join(colors_df, "color_id") \
    .select(     
        F.col("event_id"),   
        F.col("event_time"),   
        F.col("car_id"),   
        F.col("speed"),   
        F.col("rpm"),  
        F.col("gear"),
        F.col("driver_id"),            
        F.col("car_brand").alias("brand_name"),
        F.col("car_model").alias("model_name"),
        F.col("color_name"),
       (F.round(F.col("speed") / F.lit(30))).cast("int").alias("expected_gear")
    )


enriched_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("topic", "samples-enriched") \
    .option("checkpointLocation", "s3a://spark/checkpoints/enrichment") \
    .outputMode("append") \
    .start() \
    .awaitTermination()



spark.stop()
    
    
