from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType

schema = StructType() \
    .add("user", StringType()) \
    .add("product", StringType()) \
    .add("timestamp", DoubleType()) \
    .add("action", StringType())

spark = SparkSession.builder \
    .appName("EcommerceClickstream") \
    .master("local[*]") \
    .config("spark.driver.host", "spark-submit") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("✅ SparkSession created!")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "user_clicks") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp")))

# Write each batch to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    print(f"📦 Writing batch {batch_id} with {batch_df.count()} rows to PostgreSQL...")
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/clickstream") \
        .option("dbtable", "clickstream_events") \
        .option("user", "sparkuser") \
        .option("password", "sparkpass") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    print(f"✅ Batch {batch_id} written successfully!")

query = df \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .trigger(processingTime="5 seconds") \
    .start()

print("✅ Streaming started! Writing to PostgreSQL...")
query.awaitTermination()