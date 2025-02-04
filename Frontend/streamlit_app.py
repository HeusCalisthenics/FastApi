import streamlit as st
import requests

BASE_API_URL = "http://fastapi:8000/requests"  # Use FastAPI service name instead of localhost

st.title("API Logs Dashboard")

response = requests.get(BASE_API_URL)
if response.status_code == 200:
    logs = response.json()
    st.table(logs)
else:
    st.error("Failed to fetch logs")
