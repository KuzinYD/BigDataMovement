from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import coalesce
import pygeohash as pgh
import requests
import time



RESTAURANT_PATH = "restaurant_csv/"
OUTPUT_PATH = "output/enriched_restaurants"

OPENCAGE_KEY = ""
OPENCAGE_URL = "https://api.opencagedata.com/geocode/v1/json"

def geocode_address(query: str):
    if not query:
        return None, None
    try:
        resp = requests.get(OPENCAGE_URL, params={"q": query, "key": OPENCAGE_KEY, "limit": 1, "no_annotations": 1}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data["results"]:
            geo = data["results"][0]["geometry"]
            return geo["lat"], geo["lng"]
        return None, None
    except:
        return None, None

def geohash_4(lat, lng):
    if lat is None or lng is None:
        return None
    return pgh.encode(lat, lng, precision=4)

geohash_udf = udf(geohash_4, StringType())

def read_restaurants(spark, path):
    return spark.read.option("header","true").option("inferSchema","true").csv(path)



if __name__ == "__main__":
    spark = SparkSession.builder.appName("RestaurantWeatherETL").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    restaurants_df = read_restaurants(spark, RESTAURANT_PATH)
    missing_df = restaurants_df.filter(col("lat").isNull() | col("lng").isNull()).select("id", "city", "country").distinct()

    resolved_rows = []
    for r in missing_df.collect():
        query = f"{r.city}, {r.country}" if r.city and r.country else ""
        lat, lng = geocode_address(query)
        if lat is None or lng is None:
            lat, lng = 0.0, 0.0
        resolved_rows.append(Row(id=r.id, lat=lat, lng=lng))
        time.sleep(0.1)


    if resolved_rows:
        resolved_df = spark.createDataFrame(resolved_rows).withColumnRenamed("lat", "resolved_lat").withColumnRenamed("lng", "resolved_lng")
        restaurants_df = restaurants_df.join(resolved_df, on="id", how="left") \
            .withColumn("lat", coalesce(col("lat"), col("resolved_lat"))) \
            .withColumn("lng", coalesce(col("lng"), col("resolved_lng"))) \
            .drop("resolved_lat", "resolved_lng")

    restaurants_geo = restaurants_df.withColumn("geohash", geohash_udf(col("lat"), col("lng")))

    restaurants_geo.write.mode("overwrite").partitionBy("geohash").parquet(OUTPUT_PATH)

    print("Restaurant count:", restaurants_df.count())
    restaurants_geo.show(5)