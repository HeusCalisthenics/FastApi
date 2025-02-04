from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import os

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("Query Delta Tables") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define table paths
LOG_TABLE_PATH = "/app/delta_tables/logs"
CRYPTO_TABLE_PATH = "/app/delta_tables/crypto_requests"

def query_delta_table(path):
    """Reads and displays contents of a Delta table."""
    if os.path.exists(path) and DeltaTable.isDeltaTable(spark, path):
        df = spark.read.format("delta").load(path)
        print(f"\n🔍 Querying Delta Table at: {path}")
        df.show(truncate=False)
    else:
        print(f"⚠️ Delta table at {path} does not exist.")

# Query all Delta tables
query_delta_table(LOG_TABLE_PATH)
query_delta_table(CRYPTO_TABLE_PATH)

# Stop Spark session
spark.stop()
