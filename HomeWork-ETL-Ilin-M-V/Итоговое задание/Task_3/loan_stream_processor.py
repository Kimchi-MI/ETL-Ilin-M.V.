from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

spark = SparkSession.builder \
    .appName("KafkaLoanStream") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

documents_schema = ArrayType(StructType([
    StructField("type", StringType(), True),
    StructField("status", StringType(), True)
]))

loan_schema = StructType([
    StructField("amount", IntegerType(), True),
    StructField("term_months", IntegerType(), True)
])

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("region", StringType(), True)
])

scoring_schema = StructType([
    StructField("score", IntegerType(), True),
    StructField("risk_level", StringType(), True)
])

json_schema = StructType([
    StructField("application_id", StringType(), True),
    StructField("customer", customer_schema, True),
    StructField("loan", loan_schema, True),
    StructField("scoring", scoring_schema, True),
    StructField("documents", documents_schema, True),
    StructField("decision_status", StringType(), True)
])

KAFKA_BROKER = "rc1a-7sth8d2a4ltacuri.mdb.yandexcloud.net:9091"
KAFKA_TOPIC = "loan_applications"
OUTPUT_BUCKET = "s3a://de-dataproc-bucket1"

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.scram.ScramLoginModule required username="producer-user" password="Maratka10_";') \
    .load()

parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), json_schema).alias("data")) \
    .select(
        col("data.application_id").alias("application_id"),
        col("data.customer.customer_id").alias("customer_id"),
        col("data.customer.region").alias("region"),
        col("data.loan.amount").alias("loan_amount"),
        col("data.loan.term_months").alias("loan_term_months"),
        col("data.scoring.score").alias("scoring_score"),
        col("data.scoring.risk_level").alias("risk_level"),
        col("data.documents").alias("documents"),
        col("data.decision_status").alias("decision_status"),
        current_timestamp().alias("processed_at")
    )

query = parsed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", f"{OUTPUT_BUCKET}/output/loans_raw/") \
    .option("checkpointLocation", f"{OUTPUT_BUCKET}/dataproc/checkpoints/final/") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()