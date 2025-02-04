running the fastapi in combination with the streamlit

cd C:\Users\31614\OneDrive\Desktop\FastApi\FastApi
python -m app.main

cd C:\Users\31614\OneDrive\Desktop\FastApi\frontend
∙ streamlit run streamlit_app.py

#1

Docker

see running localhost scripts:
docker ps --format "table {{.ID}}\t{{.Image}}\t{{.Ports}}\t{{.Names}}"

if any page fails: use:
docker logs -f fastapi-fastapi-1
this will show the error.
