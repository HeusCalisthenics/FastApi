from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
import datetime
import os

# Define the correct database path
DATABASE_PATH = "C:/Users/31614/OneDrive/Desktop/FastApi/Database/requests.db"
DATABASE_URL = f"sqlite:///{DATABASE_PATH}"

# Create database engine
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Define API Request Log Model
class APIRequestLog(Base):
    __tablename__ = "api_requests"

    id = Column(Integer, primary_key=True, index=True)
    user_ip = Column(String, index=True)
    requested_symbol = Column(String, index=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    status_code = Column(Integer)
    success = Column(Boolean)
    response_data = Column(String)

# Create tables if they don’t exist
Base.metadata.create_all(bind=engine)
