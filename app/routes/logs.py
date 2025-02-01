from fastapi import APIRouter, HTTPException
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pydantic import BaseModel
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

# Pydantic model for input validation
class LogEntry(BaseModel):
    message: str

@router.post("/log/")
def write_log(log_entry: LogEntry):
    """Append a new log message to Delta Lake."""
    try:
        df = spark.createDataFrame([(log_entry.message,)], ["message"])
        df.write.format("delta").mode("append").save(DELTA_TABLE_PATH)
        return {"status": "success", "message": log_entry.message}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error writing log: {str(e)}")

@router.get("/logs/")
def read_logs():
    """Read all log messages from Delta Lake."""
    try:
        if not os.path.exists(DELTA_TABLE_PATH):
            return {"logs": []}  # Return empty list if table doesn't exist
        
        df = spark.read.format("delta").load(DELTA_TABLE_PATH)
        logs = df.collect()
        return {"logs": [row["message"] for row in logs]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading logs: {str(e)}")

@router.get("/logs/latest/")
def read_latest_log():
    """Get the most recent log entry from Delta Lake."""
    try:
        df = spark.read.format("delta").load(DELTA_TABLE_PATH)
        latest_log = df.orderBy(df["message"].desc()).limit(1).collect()
        return {"latest_log": latest_log[0]["message"] if latest_log else "No logs found"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching latest log: {str(e)}")
