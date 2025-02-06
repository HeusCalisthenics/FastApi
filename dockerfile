# Use the latest official Spark image with Java 11 and Scala 2.12
FROM apache/spark:3.5.0-scala2.12-java11-ubuntu

USER root

# Install required dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip \
    openjdk-11-jdk \
    curl && \
    rm -rf /var/lib/apt/lists/*

# Install Delta JARs correctly
RUN mkdir -p /opt/spark/jars/ && \
    curl -L -o /opt/spark/jars/delta-core_2.12-2.4.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar

# Set Spark environment variables to load the Delta JAR
# ENV SPARK_CLASSPATH="/opt/spark/jars/delta-core_2.12-2.4.0.jar"
ENV SPARK_CLASSPATH="/opt/spark/jars/*"

# Set working directory to Spark JARs folder
WORKDIR /opt/spark/jars/

# Download required Delta JARs
# Install the correct Delta version for Spark 3.5.0
RUN curl -L -o /opt/spark/jars/delta-spark_2.12-3.2.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
    && curl -L -o /opt/spark/jars/delta-storage-3.2.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar


# Set working directory
WORKDIR /app

# ✅ Set PYTHONPATH so modules inside /app can be imported
ENV PYTHONPATH=/app


# Copy requirements and install Python dependencies
COPY requirements.txt /app/
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY keys.py /app/
COPY app /app/
COPY app /app/app


# Copy the application code
# COPY app /app/app
# COPY database /app/database
# COPY app/spark_session.py /app/
# COPY app/initialize_delta_tables.py /app/

COPY app/scripts /app/scripts
COPY data /app/data

# Copy initialization script

# Run the table initialization before starting FastAPI
RUN python3 /app/initialize_delta_tables.py
RUN python3 /app/degiro.py
# Run FastAPI on startup
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
