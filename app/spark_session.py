# app/spark_session.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("FastAPI Delta Lake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")