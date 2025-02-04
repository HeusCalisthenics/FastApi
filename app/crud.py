from spark_session import spark
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, BooleanType
from datetime import datetime, timezone
from delta.tables import DeltaTable

DELTA_LOGS_PATH = "/app/delta_tables/api_logs"

log_schema = StructType([
    StructField("user_ip", StringType(), True),
    StructField("requested_symbol", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("status_code", IntegerType(), True),
    StructField("success", BooleanType(), True),
    StructField("response_data", StringType(), True)
])



def delta_table_exists(path):
    """Check if a Delta table exists at the given path."""
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False  # If an error occurs, assume the table doesn't exist.

def log_request(user_ip: str, symbol: str, status_code: int, success: bool, response_data: str):
    """Logs API requests into Delta table, ensuring it exists first"""
    try:
        log_entry = {
            "user_ip": user_ip,
            "requested_symbol": symbol.upper(),
            "timestamp": datetime.now(timezone.utc),
            "status_code": status_code,
            "success": success,
            "response_data": response_data[:500]  # Limit response size
        }

        # Convert to Spark DataFrame
        df = spark.createDataFrame([log_entry], schema=log_schema)

        # ✅ If Delta table exists, append. Otherwise, create it.
        if delta_table_exists(DELTA_LOGS_PATH):
            df.write.format("delta").mode("append").option("mergeSchema", "true").save(DELTA_LOGS_PATH)
            print(f"✅ Data appended to Delta table: {log_entry}")
        else:
            df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(DELTA_LOGS_PATH)
            print(f"✅ Delta table created and first entry written: {log_entry}")

    except Exception as e:
        print(f"⚠️ Error storing log in Delta: {e}")

# Function to get all API logs
def get_logs():
    """Reads all logs from the Delta table"""
    try:
        df = spark.read.format("delta").load(DELTA_LOGS_PATH)
        return df.toPandas().to_dict(orient="records")
    except Exception as e:
        print(f"⚠️ Error reading logs: {e}")
        return []
