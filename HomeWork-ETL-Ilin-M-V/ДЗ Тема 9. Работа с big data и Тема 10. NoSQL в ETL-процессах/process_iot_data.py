from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, count, isnan, isnull
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("IoT Data Processing") \
    .getOrCreate()

# Схемы данных
schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("datetime", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("altitude", DoubleType(), True),
    StructField("speed", DoubleType(), True),
    StructField("battery_voltage", DoubleType(), True),
    StructField("cabin_temperature", IntegerType(), True),
    StructField("fuel_level", DoubleType(), True)
])

# Чтение данных S3
input_path = "s3a://loop-dataproc-bucket/sample.json"
df = spark.read.schema(schema).json(input_path)

df.show()

df.printSchema()

df_cleaned = df \
    .fillna({"speed": 0, "battery_voltage": 0, "fuel_level": 0}) \
    .fillna({"cabin_temperature": 20})

df_enriched = df_cleaned.withColumn(
    "speed_category",
    when(col("speed") == 0, "stopped")
     .when(col("speed") <= 30, "low")
     .when(col("speed") <= 80, "medium")
     .otherwise("high")
)

df_aggregated = df_enriched.groupBy("device_id").agg(
    avg("speed").alias("avg_speed"),
    avg("battery_voltage").alias("avg_battery_voltage"),
    count("datetime").alias("readings_count")
)

df_aggregated.show()

output_path = "s3a://loop-dataproc-bucket/processed_results/"
df_aggregated.write.mode("overwrite").parquet(output_path)
df_enriched.coalesce(1).write.mode("overwrite").json(output_path + "enriched/")

df_verification = spark.read.parquet(output_path)
df_verification.show()

spark.stop()