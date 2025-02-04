from fastapi import APIRouter, HTTPException, Request
import requests

from ..crud import log_request
import sys
import os
from datetime import datetime, timezone
from spark_session import spark


# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Import `keys.py` correctly
from keys import coinstats_api

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

router = APIRouter()



# Delta table path for crypto API logs
CRYPTO_TABLE_PATH = "/app/delta_tables/crypto_requests"
DELTA_API_LOGS_PATH = "/app/delta_tables/api_logs"

# Define schema for Delta table
crypto_schema = StructType([
    StructField("ip_address", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
])

# Function to fetch cryptocurrency data from CoinStats
def get_crypto_data(symbol: str):
    url = f"https://openapiv1.coinstats.app/coins?symbol={symbol.upper()}"
    headers = {
        "Accept": "application/json",
        "X-API-KEY": coinstats_api
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json(), response.status_code
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Error fetching CoinStats data: {str(e)}")

# Function to store API request data in Delta table

def store_crypto_request(ip_address: str, symbol: str, price: float):
    try:
        crypto_info = {
            "ip_address": ip_address,
            "symbol": symbol.upper(),
            "price": price,
            "timestamp": datetime.now(timezone.utc)  
        }

        # Convert to Spark DataFrame
        df = spark.createDataFrame([crypto_info], schema=crypto_schema)

        # Append data to Delta table
        df.write.format("delta").mode("append").save(CRYPTO_TABLE_PATH)

        print(f"✅ Data written: {crypto_info} at {CRYPTO_TABLE_PATH}")  # DEBUGGING

    except Exception as e:
        print(f"⚠️ Error storing data in Delta: {e}")

# Crypto Price Endpoint
@router.get("/crypto/{symbol}")
async def get_crypto(symbol: str, request: Request):
    user_ip = request.client.host

    try:
        response_data, status_code = get_crypto_data(symbol)

        # Extract price if available
        price = response_data.get("coin", {}).get("price", None)

        # Store request in Delta Lake
        if price is not None:
            store_crypto_request(user_ip, symbol, price)

        # Log request in SQL database
        log_request( user_ip, symbol, status_code, True, str(response_data))

        return response_data
    except HTTPException as e:
        # Log failed request
        log_request( user_ip, symbol, e.status_code, False, str(e.detail))
        raise e

# Endpoint to retrieve stored crypto requests from Delta Lake
@router.get("/requests")
def get_stored_crypto_requests():
    try:
        df = spark.read.format("delta").load(DELTA_API_LOGS_PATH)
        return df.toPandas().to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading stored crypto data: {str(e)}")
