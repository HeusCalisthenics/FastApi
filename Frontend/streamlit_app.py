import streamlit as st
import requests

BASE_API_URL = "http://127.0.0.1:8000/logs/json/"

st.title("API Logs Dashboard")

response = requests.get(BASE_API_URL)
if response.status_code == 200:
    logs = response.json()
    st.table(logs)
else:
    st.error("Failed to fetch logs")
