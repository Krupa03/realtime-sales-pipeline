from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("order_id",    StringType(),  True),
    StructField("customer_id", StringType(),  True),
    StructField("product",     StringType(),  True),
    StructField("quantity",    IntegerType(), True),
    StructField("price",       DoubleType(),  True),
    StructField("region",      StringType(),  True),
    StructField("status",      StringType(),  True),
    StructField("timestamp",   StringType(),  True),
])

spark = SparkSession.builder \
    .appName("RealTimeSalesPipeline") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*") \
 .withColumn("order_time", to_timestamp(col("timestamp")))

def write_to_timescale(batch_df, batch_id):
    batch_df.select(
        "order_id", "customer_id", "product",
        "quantity", "price", "region", "status", "order_time"
    ).write \
     .format("jdbc") \
     .option("url", "jdbc:postgresql://localhost:5432/sales_db") \
     .option("dbtable", "orders") \
     .option("user", "admin") \
     .option("password", "password") \
     .option("driver", "org.postgresql.Driver") \
     .mode("append") \
     .save()
    print(f"Batch {batch_id} written to TimescaleDB")

query = parsed.writeStream \
    .foreachBatch(write_to_timescale) \
    .outputMode("append") \
    .start()

query.awaitTermination()