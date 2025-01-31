from fastapi import APIRouter, Depends, HTTPException, Request
import requests
from sqlalchemy.orm import Session
from ..database import SessionLocal
from ..crud import log_request

import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Import `keys.py` correctly
from keys import coinstats_api


router = APIRouter()

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

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

# Crypto Price Endpoint
@router.get("/crypto/{symbol}")
async def get_crypto(symbol: str, request: Request, db: Session = Depends(get_db)):
    user_ip = request.client.host

    try:
        response_data, status_code = get_crypto_data(symbol)

        # Log request
        log_request(db, user_ip, symbol, status_code, True, str(response_data))

        return response_data
    except HTTPException as e:
        # Log failed request
        log_request(db, user_ip, symbol, e.status_code, False, str(e.detail))
        raise e
