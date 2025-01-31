from fastapi import FastAPI, HTTPException, Depends, Request
import requests
import datetime
from keys import coinstats_api  # Importing API Key
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from fastapi.responses import HTMLResponse


app = FastAPI()

# Correct database path (update based on your actual path)
DATABASE_PATH = "C:/Users/31614/OneDrive/Desktop/FastApi/Database/requests.db"
DATABASE_URL = f"sqlite:///{DATABASE_PATH}"

# Database setup
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Database Model to Log API Requests
class APIRequestLog(Base):
    __tablename__ = "api_requests"

    id = Column(Integer, primary_key=True, index=True)
    user_ip = Column(String, index=True)  # IP address of requester
    requested_symbol = Column(String, index=True)  # Requested crypto symbol
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)  # When request was made
    status_code = Column(Integer)  # HTTP Status code (200, 400, etc.)
    success = Column(Boolean)  # True if successful, False otherwise
    response_data = Column(String)  # Store response data (optional)

# Create database table if it doesn’t exist
Base.metadata.create_all(bind=engine)

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

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Root Endpoint
@app.get("/")
def root():
    return {"message": "Welcome to the CoinStats API! Use /crypto/{symbol} to fetch cryptocurrency data, and /logs/ to view API logs."}

# API Endpoint to Retrieve Cryptocurrency Data & Log Request
@app.get("/crypto/{symbol}")
async def get_crypto(symbol: str, request: Request, db: Session = Depends(get_db)):
    user_ip = request.client.host  # Get user's IP address

    try:
        # Fetch crypto data
        response_data, status_code = get_crypto_data(symbol)

        # Save request details to database
        log_entry = APIRequestLog(
            user_ip=user_ip,
            requested_symbol=symbol.upper(),
            status_code=status_code,
            success=True,
            response_data=str(response_data)[:500]  # Limit stored response size
        )
        db.add(log_entry)
        db.commit()
        db.refresh(log_entry)

        return response_data
    except HTTPException as e:
        # Log failed request
        log_entry = APIRequestLog(
            user_ip=user_ip,
            requested_symbol=symbol.upper(),
            status_code=e.status_code,
            success=False,
            response_data=str(e.detail)
        )
        db.add(log_entry)
        db.commit()
        db.refresh(log_entry)

        raise e

# API Endpoint to Retrieve Logs (View Requests Easily)
@app.get("/logs/", response_class=HTMLResponse)
def get_logs_html(db: Session = Depends(get_db)):
    logs = db.query(APIRequestLog).all()

    # Dark theme HTML & CSS
    table_html = """
    <html>
    <head>
        <title>API Request Logs</title>
        <style>
            body {
                background-color: #121212;
                color: white;
                font-family: Arial, sans-serif;
            }
            table { 
                width: 100%%; 
                border-collapse: collapse; 
                margin-top: 20px;
            }
            th, td { 
                border: 1px solid #333; 
                padding: 8px; 
                text-align: left; 
            }
            th { 
                background-color: #1E1E1E; 
                color: cyan; 
                font-weight: bold; 
            }
            td { 
                color: yellow;
            }
            tr:nth-child(even) { 
                background-color: #1E1E1E;
            }
            tr:hover {
                background-color: #333;
            }
            h2 {
                color: cyan;
                text-align: center;
            }
        </style>
    </head>
    <body>
        <h2>API Request Logs</h2>
        <table>
            <tr>
                <th>ID</th>
                <th>User IP</th>
                <th>Requested Symbol</th>
                <th>Timestamp</th>
                <th>Status Code</th>
                <th>Success</th>
                <th>Response Data</th>
            </tr>
    """

    # Add rows to the table
    for log in logs:
        table_html += f"""
            <tr>
                <td>{log.id}</td>
                <td>{log.user_ip}</td>
                <td>{log.requested_symbol}</td>
                <td>{log.timestamp}</td>
                <td>{log.status_code}</td>
                <td>{'✅' if log.success else '❌'}</td>
                <td>{log.response_data[:100]}...</td>
            </tr>
        """

    table_html += """
        </table>
    </body>
    </html>
    """

    return table_html
# Run the API
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
