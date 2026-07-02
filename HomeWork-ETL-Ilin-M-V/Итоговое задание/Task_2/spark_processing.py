import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--processing_date", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("DE-Processing-Task") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    try:
        print(f"Reading data from: {args.input_path}")
        
        schema = StructType([
            StructField("application_id", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("region_code", StringType(), True),
            StructField("product_type", StringType(), True),
            StructField("requested_amount", IntegerType(), True),
            StructField("term_months", IntegerType(), True),
            StructField("credit_score", IntegerType(), True),
            StructField("risk_level", StringType(), True),
            StructField("decision_status", StringType(), True),
            StructField("approved_amount", IntegerType(), True),
            StructField("channel", StringType(), True),
            StructField("employee_review_flag", StringType(), True),
            StructField("processing_time_sec", IntegerType(), True)
        ])

        df = spark.read \
            .option("header", "true") \
            .schema(schema) \
            .csv(args.input_path)

        print(f"Rows loaded: {df.count()}")
        print("Sample data:")
        df.show(5)

        df = df.withColumn(
            "risk_category",
            when(col("credit_score") < 450, "High Risk")
            .when((col("credit_score") >= 450) & (col("credit_score") < 600), "Medium Risk")
            .otherwise("Low Risk")
        )

        df = df.withColumn(
            "is_approved",
            when(col("decision_status") == "approved", True).otherwise(False)
        )

        df = df.withColumn("processing_date", lit(args.processing_date))

        agg_df = df.groupBy("product_type").agg(
            {"requested_amount": "avg", "credit_score": "avg", "processing_time_sec": "avg"}
        )
        agg_df = agg_df.withColumnRenamed("avg(requested_amount)", "avg_requested") \
                       .withColumnRenamed("avg(credit_score)", "avg_credit_score") \
                       .withColumnRenamed("avg(processing_time_sec)", "avg_processing_time")

        print("Aggregation results:")
        agg_df.show()


        df.write \
            .mode("overwrite") \
            .parquet(f"{args.output_path}/data")


        agg_df.write \
            .mode("overwrite") \
            .parquet(f"{args.output_path}/aggregated")

    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()