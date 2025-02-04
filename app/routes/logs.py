from fastapi import APIRouter, HTTPException, Request
from pyspark.sql.functions import current_timestamp
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pydantic import BaseModel
from datetime import datetime, timezone
import os

from initialize_delta_tables import LOG_TABLE_PATH

from spark_session import spark

router = APIRouter()

# Delta Table Storage Path
# LOG_TABLE_PATH = "/app/delta_tables/logs"
os.makedirs(LOG_TABLE_PATH, exist_ok=True)




# Define LogEntry Pydantic Model
# class LogEntry(BaseModel):
#     message: str

async def log_api_call(request: Request, call_next):
    """Middleware to log all API requests into Delta Lake."""
    start_time = datetime.now(timezone.utc)
    ip_address = request.client.host
    endpoint = request.url.path
    method = request.method

    response = await call_next(request)  # Continue processing the request

    status_code = response.status_code
    log_entry = {
        "ip_address": ip_address,
        "endpoint": endpoint,
        "method": method,
        "status_code": status_code,
        "timestamp": start_time,
    }

    # Convert to Spark DataFrame
    df = spark.createDataFrame([log_entry])

    # ✅ Append to Delta table
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(LOG_TABLE_PATH)

    print(f"✅ API Log Stored: {log_entry}")  # Debugging

    return response

# ✅ Apply the middleware globally
def register_logging_middleware(app):
    app.middleware("http")(log_api_call)




@router.get("/logs/")
def read_logs():
    """Read all log messages from Delta Lake."""
    try:
        if not os.path.exists(LOG_TABLE_PATH):
            return {"logs": []}  # Return empty list if table doesn't exist
        
        df = spark.read.format("delta").load(LOG_TABLE_PATH)
        logs = df.collect()
        return {
            "logs": [
                {
                    "ip_address": row["ip_address"],
                    "endpoint": row["endpoint"],
                    "method": row["method"],
                    "status_code": row["status_code"],
                    "timestamp": row["timestamp"]
                } 
                for row in logs
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading logs: {str(e)}")

@router.get("/logs/latest/")
def read_latest_log():
    """Get the most recent log entry from Delta Lake."""
    try:
        df = spark.read.format("delta").load(LOG_TABLE_PATH)
        latest_log = df.orderBy(df["timestamp"].desc()).limit(1).collect()
        return {"latest_log": latest_log[0]["message"] if latest_log else "No logs found"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching latest log: {str(e)}")
