FROM python:3.10

WORKDIR /app

COPY app /app/app
COPY database /app/database
COPY keys.py /app/
COPY requirements.txt /app/

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
