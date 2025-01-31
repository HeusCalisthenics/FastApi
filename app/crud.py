from sqlalchemy.orm import Session
from .database import APIRequestLog

# Function to log API requests
def log_request(db: Session, user_ip: str, symbol: str, status_code: int, success: bool, response_data: str):
    log_entry = APIRequestLog(
        user_ip=user_ip,
        requested_symbol=symbol.upper(),
        status_code=status_code,
        success=success,
        response_data=response_data[:500]  # Limit stored response size
    )
    db.add(log_entry)
    db.commit()
    db.refresh(log_entry)
    return log_entry

# Function to get all API logs
def get_logs(db: Session):
    return db.query(APIRequestLog).all()
