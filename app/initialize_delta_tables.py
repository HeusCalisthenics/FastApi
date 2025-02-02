import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FastAPI Delta Lake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define Delta table path
CRYPTO_TABLE_PATH = "/app/delta_tables/crypto_requests"

# Define schema
crypto_schema = StructType([
    StructField("ip_address", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
])

# Check if the Delta table already exists
if not os.path.exists(CRYPTO_TABLE_PATH):
    # Create an empty DataFrame and save as Delta table
    df = spark.createDataFrame([], schema=crypto_schema)
    df.write.format("delta").mode("overwrite").save(CRYPTO_TABLE_PATH)
    print(f"✅ Crypto Delta table initialized at: {CRYPTO_TABLE_PATH}")
else:
    print(f"🔄 Delta table already exists at: {CRYPTO_TABLE_PATH}, skipping initialization.")
