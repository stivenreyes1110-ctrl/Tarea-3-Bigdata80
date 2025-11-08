
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg as _avg, count as _count,
    to_timestamp, coalesce, round as _round
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "nyc_taxi_trips")

schema = StructType([
    StructField("tpep_pickup_datetime",  StringType()),
    StructField("tpep_dropoff_datetime", StringType()),
    StructField("PULocationID",          StringType()),
    StructField("DOLocationID",          StringType()),
    StructField("trip_distance",         DoubleType()),
    StructField("fare_amount",           DoubleType()),
    StructField("tip_amount",            DoubleType()),
    StructField("total_amount",          DoubleType()),
    StructField("passenger_count",       IntegerType()),
])

spark = (SparkSession.builder
         .appName("NYC_Taxi_Streaming_Consumer")
         .config("spark.sql.shuffle.partitions", "50")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "latest")
       .load())

json_df = raw.selectExpr("CAST(value AS STRING)")
df = json_df.select(from_json(col("value"), schema).alias("d")).select("d.*")

df = (df
      .withColumn("pickup_ts",  to_timestamp("tpep_pickup_datetime"))
      .withColumn("dropoff_ts", to_timestamp("tpep_dropoff_datetime"))
      .withColumn("pickup_zone",  coalesce(col("PULocationID"), col("pulocationid")).cast("int"))
      .withColumn("dropoff_zone", coalesce(col("DOLocationID"), col("dolocationid")).cast("int"))
      .where(col("pickup_ts").isNotNull() & col("pickup_zone").isNotNull())
)

agg = (df.withWatermark("pickup_ts", "15 minutes")
       .groupBy(window(col("pickup_ts"), "5 minutes", "1 minute"),
                col("pickup_zone"))
       .agg(_count("*").alias("trips"),
            _avg("fare_amount").alias("avg_fare"),
            _avg("trip_distance").alias("avg_distance")))

out_console = (agg.select(
        col("window.start").cast("string").alias("win_start"),
        col("window.end").cast("string").alias("win_end"),
        "pickup_zone",
        "trips",
        _round("avg_fare", 2).alias("avg_fare"),
        _round("avg_distance", 2).alias("avg_distance"),
    )
    .writeStream.outputMode("update")
    .format("console")
    .option("truncate", "false")
    .start()
)

out_parquet = (agg
    .writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", "data/processed/stream_agg/")
    .option("checkpointLocation", "data/checkpoints/stream_agg/")
    .start()
)

spark.streams.awaitAnyTermination()
PY
chmod +x streaming/stream_consumer_spark.py
