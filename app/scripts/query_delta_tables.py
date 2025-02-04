from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import pandas as pd

# ✅ Configure Spark
builder = (
    SparkSession.builder
    .appName("Delta Table Query")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# ✅ Define Delta Table Paths
DELTA_TABLES = {
    "logs": "/app/delta_tables/logs",
    "crypto_requests": "/app/delta_tables/crypto_requests"
}

# ✅ Query and Display Each Table
for table_name, path in DELTA_TABLES.items():
    print(f"\n🔍 Querying Delta Table: {table_name} ({path})")
    try:
        df = spark.read.format("delta").load(path)
        pandas_df = df.toPandas()  # Convert to Pandas for better visualization
        print(pandas_df)  # This prints it neatly instead of raw Spark output
    except Exception as e:
        print(f"❌ Error reading table {table_name}: {e}")


pandas_df.to_csv("/app/scripts/logs_output.csv", index=False)
print("✅ Logs saved to logs_output.csv")
