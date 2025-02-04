from fastapi import FastAPI
from app.routes.crypto import router as crypto_router
from app.routes.logs import router as logs_router
from app.routes.logs import router as log_router, register_logging_middleware
from spark_session import spark


app = FastAPI()

# Include Routers
app.include_router(crypto_router)
app.include_router(logs_router)
# ✅ Register the logging middleware
register_logging_middleware(app)

# ✅ Include routes
app.include_router(log_router)

@app.get("/")
def root():
    return {"message": "Welcome to the CoinStats API! Use /crypto/{symbol} to fetch cryptocurrency data, and /logs/ to view API logs."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
