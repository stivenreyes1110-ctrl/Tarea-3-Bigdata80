
import os, glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, avg as _avg, count as _count, sum as _sum

RAW_GLOB     = os.getenv("RAW_GLOB", "data/raw/tripdata_2019-*.csv")
OUT_PARQUET  = os.getenv("OUT_PARQUET", "data/processed/trips_parquet/")
OUT_SUMMARY  = os.getenv("OUT_SUMMARY", "data/processed/summary_by_zone.csv")

spark = (SparkSession.builder
         .appName("NYC_Taxi_Batch_ETL")
         .config("spark.sql.shuffle.partitions", "200")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

print(f"→ Buscando archivos: {RAW_GLOB}")
paths = glob.glob(RAW_GLOB)
if not paths:
    raise SystemExit("No se encontraron CSV. Ajusta RAW_GLOB o coloca archivos en data/raw/")

df = (spark.read.option("header", True).csv(paths))

df = (df
      .withColumn("pickup_ts",  to_timestamp("tpep_pickup_datetime"))
      .withColumn("dropoff_ts", to_timestamp("tpep_dropoff_datetime"))
      .withColumn("trip_distance", col("trip_distance").cast("double"))
      .withColumn("fare_amount",   col("fare_amount").cast("double"))
      .withColumn("tip_amount",    col("tip_amount").cast("double"))
      .withColumn("total_amount",  col("total_amount").cast("double"))
      .withColumn("passenger_count", col("passenger_count").cast("int"))
      .withColumn("PULocationID", col("PULocationID").cast("int"))
      .withColumn("DOLocationID", col("DOLocationID").cast("int"))
      .where(col("pickup_ts").isNotNull())
)

df = df.where(
    (col("fare_amount").isNotNull()) &
    (col("fare_amount") >= 0) &
    (col("trip_distance") >= 0) &
    (col("trip_distance") <= 200)
)

(df.repartition(64)
   .write.mode("overwrite")
   .parquet(OUT_PARQUET))

summary = (df.groupBy("PULocationID")
             .agg(_count("*").alias("trips"),
                  _avg("fare_amount").alias("avg_fare"),
                  _avg("trip_distance").alias("avg_distance"),
                  _sum("total_amount").alias("sum_total"))
          )
summary.coalesce(1).write.mode("overwrite").option("header", True).csv(OUT_SUMMARY)

print("✓ Batch ETL completado.")
spark.stop()
PY
chmod +x spark_batch/batch_etl.py
