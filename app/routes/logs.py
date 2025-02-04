from fastapi import APIRouter, HTTPException
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pydantic import BaseModel, Field
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from delta.tables import DeltaTable
import os

router = APIRouter()

# Define Delta Table Storage Path
DELTA_TABLE_PATH = "/app/delta_tables/logs"

# Ensure Delta Lake directory exists
os.makedirs(DELTA_TABLE_PATH, exist_ok=True)

# ✅ Configure Spark with Delta Storage explicitly
builder = (
    SparkSession.builder
    .appName("FastAPI + Delta Lake")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars", "/opt/spark/jars/delta-core_2.12-2.4.0.jar,/opt/spark/jars/delta-spark_2.12-3.3.0.jar,/opt/spark/jars/delta-storage-2.4.0.jar")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# ✅ Define the latest schema for logs
log_schema = StructType([
    StructField("message", StringType(), True),
    StructField("timestamp", TimestampType(), True),
])

def initialize_or_upgrade_delta_table(path, schema):
    """Check if the Delta table exists, compare schemas, and update if necessary."""
    if DeltaTable.isDeltaTable(spark, path):
        # ✅ Table exists, check schema
        delta_table = DeltaTable.forPath(spark, path)
        existing_schema = delta_table.toDF().schema

        # ✅ Compare schemas and add missing columns
        new_columns = [field for field in schema if field not in existing_schema]

        if new_columns:
            print(f"🛠 Upgrading schema for Delta table at: {path}")
            for col in new_columns:
                spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMNS ({col.name} {col.dataType.simpleString()})")
            print(f"✅ Schema updated at: {path}")
        else:
            print(f"🔄 Delta table schema is already up to date at: {path}, skipping upgrade.")
    else:
        # ✅ Table doesn't exist, create it from scratch
        print(f"🚀 Creating Delta table at: {path}")
        df = spark.createDataFrame([], schema=schema)
        df.write.format("delta").mode("overwrite").save(path)
        print(f"✅ Delta table initialized at: {path}")

# ✅ Initialize or upgrade Delta table schema
initialize_or_upgrade_delta_table(DELTA_TABLE_PATH, log_schema)

# ✅ Updated Pydantic Model with Optional Timestamp
class LogEntry(BaseModel):
    message: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)

@router.post("/log/")
def write_log(log_entry: LogEntry):
    """Append a new log message to Delta Lake with a timestamp."""
    try:
        df = spark.createDataFrame(
            [(log_entry.message, log_entry.timestamp)],
            schema=log_schema
        )
        df.write.format("delta").mode("append").save(DELTA_TABLE_PATH)
        return {"status": "success", "message": log_entry.message, "timestamp": log_entry.timestamp}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error writing log: {str(e)}")

@router.get("/logs/")
def read_logs():
    """Read all log messages from Delta Lake."""
    try:
        if not DeltaTable.isDeltaTable(spark, DELTA_TABLE_PATH):
            return {"logs": []}  # Return empty list if table doesn't exist
        
        df = spark.read.format("delta").load(DELTA_TABLE_PATH)
        logs = df.collect()
        return {"logs": [{"message": row["message"], "timestamp": row["timestamp"]} for row in logs]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading logs: {str(e)}")

@router.get("/logs/latest/")
def read_latest_log():
    """Get the most recent log entry from Delta Lake."""
    try:
        df = spark.read.format("delta").load(DELTA_TABLE_PATH)
        latest_log = df.orderBy(df["timestamp"].desc()).limit(1).collect()
        return {
            "latest_log": latest_log[0]["message"] if latest_log else "No logs found",
            "timestamp": latest_log[0]["timestamp"] if latest_log else None
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching latest log: {str(e)}")
