from fastapi import FastAPI, HTTPException
import requests
import os

app = FastAPI()

# CoinStats API Endpoint (modify if needed)
COINSTATS_API_URL = "https://api.coinstats.app/public/v1/portfolio?uid=RfO76K0b6aYGNG3"

# Function to fetch data from CoinStats
def get_coinstats_data():
    try:
        response = requests.get(COINSTATS_API_URL)
        response.raise_for_status()  # Raises an error for bad responses (4xx, 5xx)
        return response.json()  # Return JSON response
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Error fetching CoinStats data: {str(e)}")

# API Endpoint to Retrieve Financial Data from CoinStats
@app.get("/portfolio/")
def get_portfolio():
    return get_coinstats_data()

# Run the API
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
