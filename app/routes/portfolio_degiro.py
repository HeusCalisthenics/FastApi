from fastapi import APIRouter, HTTPException, Request
import pandas as pd
import sys
import os
from spark_session import spark


# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Import `keys.py` correctly


router = APIRouter()

DELTA_TABLE_PATH = "/app/delta_tables/portfolio_degiro"


# Endpoint to retrieve stored crypto requests from Delta Lake

@router.get("/degiro")
def get_stored_crypto_requests():
    try:
        df = spark.read.format("delta").load(DELTA_TABLE_PATH)
        
        # ✅ Replace NaN and infinite values to avoid JSON serialization issues
        pdf = df.toPandas().replace([float("inf"), float("-inf")], None)
        pdf = pdf.fillna("N/A")  # Convert NaN to a valid string
        
        return pdf.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading stored Degiro data: {str(e)}")
