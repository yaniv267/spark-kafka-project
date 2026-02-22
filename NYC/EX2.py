from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
import requests
import time
import pandas as pd

# 1️⃣ יצירת SparkSession
spark = SparkSession.builder \
    .appName("NYC_Geocode_Kafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# 2️⃣ סכימה של השדות הרלוונטיים
schema = StructType([
    StructField("house_number", StringType(), True),
    StructField("street_name", StringType(), True),
    StructField("violation_county", StringType(), True)
])

# 3️⃣ קריאה מקפקא
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9093") \
    .option("subscribe", "nyc_parking_data_last_date") \
    .option("startingOffsets", "earliest") \
    .load()

# המרת value ל-STRING
df = df.selectExpr("CAST(value AS STRING) as json_str")

# פענוח JSON לפי הסכימה
from pyspark.sql.functions import from_json
parsed_df = df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# 4️⃣ שמירת כתובות ייחודיות ל-geocoding
addresses_df = parsed_df.select("house_number", "street_name", "violation_county").distinct()

# המרת DataFrame ל-Pandas לצורך geocoding
addresses_pd = addresses_df.toPandas()

# 5️⃣ פונקציית geocoding
def geocode_address(row):
    borough_map = {"K": "Brooklyn", "M": "Manhattan", "Q": "Queens", "B": "Bronx", "R": "Staten Island"}
    borough = borough_map.get(row['violation_county'], "NY")
    if not row['house_number'] or not row['street_name']:
        return pd.Series([None, None])
    
    address = f"{row['house_number']} {row['street_name']}, {borough}, New York City"
    try:
        url = "https://nominatim.openstreetmap.org/search"
        params = {"q": address, "format": "json", "limit": 1}
        headers = {"User-Agent": "nyc-data-project"}
        r = requests.get(url, params=params, headers=headers, timeout=5)
        data = r.json()
        time.sleep(1)  # לשמור על מגבלת Rate Limit
        if data:
            return pd.Series([float(data[0]['lat']), float(data[0]['lon'])])
    except:
        return pd.Series([None, None])
    return pd.Series([None, None])

# 6️⃣ ביצוע geocoding
addresses_pd[['lat', 'lon']] = addresses_pd.apply(geocode_address, axis=1)

# 7️⃣ המרת תוצאות חזרה ל-Spark
geo_spark_df = spark.createDataFrame(addresses_pd)

# 8️⃣ join עם ה-DataFrame המקורי
df_geocoded = parsed_df.join(geo_spark_df, on=["house_number", "street_name", "violation_county"], how="left")

# 9️⃣ הצגה בקונסול
df_geocoded.select("house_number", "street_name", "violation_county", "lat", "lon").show(50, truncate=False)

spark.stop()

