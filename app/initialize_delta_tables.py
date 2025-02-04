from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType , DoubleType
from delta.tables import DeltaTable
from spark_session import spark



# Define table paths
LOG_TABLE_PATH = "/app/delta_tables/logs"
CRYPTO_TABLE_PATH = "/app/delta_tables/crypto_requests"

# Define schema for logs
log_schema = StructType([
    StructField("message", StringType(), True),
    StructField("timestamp", TimestampType(), True),
])

# Define schema for crypto requests
crypto_schema = StructType([
    StructField("ip_address", StringType(), True),  # ✅ Missing in your original schema
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),  # ✅ Should be DoubleType() not StringType()
    StructField("timestamp", TimestampType(), True),
])


def delta_table_exists(spark, path):
    """Check if a Delta table exists at the given path."""
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False  # If an error occurs, assume the table doesn't exist.

def initialize_delta_table(path, schema):
    """Check if the Delta table exists, and create it if not."""
    if not delta_table_exists(spark, path):
        df = spark.createDataFrame([], schema=schema)
        df.write.format("delta").mode("ignore").save(path)
        print(f"✅ Delta table initialized at: {path}")
    else:
        print(f"🔄 Delta table already exists at: {path}, skipping initialization.")

# Initialize both Delta tables
initialize_delta_table(LOG_TABLE_PATH, log_schema)
initialize_delta_table(CRYPTO_TABLE_PATH, crypto_schema)
