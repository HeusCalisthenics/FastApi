from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from delta.tables import DeltaTable
from pyspark.sql.functions import col, regexp_replace, current_timestamp

from spark_session import spark


# File paths
CSV_PATH = "/app/data/portfolio_degiro.csv"
DEGIRO_TABLE_PATH = "/app/delta_tables/portfolio_degiro"
CHECKPOINT_LOCATION = "/app/delta_tables/checkpoints_portfolio"

# Define schema for portfolio CSV
portfolio_schema = StructType([
    StructField("Product", StringType(), True),
    StructField("Symbool_ISIN", StringType(), True),
    StructField("Aantal", DoubleType(), True), 
    StructField("Slotkoers", DoubleType(), True),
    StructField("Lokale_waarde", StringType(), True),
    StructField("Waarde_in_EUR", StringType(), True)
    # StructField("timestamp", TimestampType(), True)  
])

# Load CSV into Spark DataFrame
df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(portfolio_schema) \
    .load(CSV_PATH)

# Ensure Delta table exists
if not DeltaTable.isDeltaTable(spark, DEGIRO_TABLE_PATH):
    df.write.format("delta") \
        .mode("overwrite") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .save(DEGIRO_TABLE_PATH)
    print(f"✅ Created new Delta table at {DEGIRO_TABLE_PATH}")


df = df.withColumn("Waarde_in_EUR", regexp_replace(col("Waarde_in_EUR"), ",", ".").cast("double")) \
    #    .withColumn("timestamp", current_timestamp())  # Add timestamp for tracking updates

# Load Delta table
delta_table = DeltaTable.forPath(spark, DEGIRO_TABLE_PATH)

# Merge new CSV data into Delta table
delta_table.alias("existing") \
    .merge(
        df.alias("new"),
        "existing.Symbool_ISIN = new.Symbool_ISIN"
    ) \
    .whenMatchedUpdate(set={
        "existing.Aantal": "new.Aantal",
        "existing.Slotkoers": "new.Slotkoers",
        "existing.Waarde_in_EUR": "new.Waarde_in_EUR"
        # "existing.timestamp": "new.timestamp"
    }) \
    .whenNotMatchedInsert(values={
        "Product": "new.Product",
        "Symbool_ISIN": "new.Symbool_ISIN",
        "Aantal": "new.Aantal",
        "Slotkoers": "new.Slotkoers",
        "Lokale_waarde": "new.Lokale_waarde",
        "Waarde_in_EUR": "new.Waarde_in_EUR"
        # "timestamp": "new.timestamp"
    }) \
    .execute()


print("✅ Portfolio data successfully updated in Delta Lake.")

# Run optimization
def optimize_portfolio_delta():
    try:
        spark.sql(f"OPTIMIZE delta.`{DEGIRO_TABLE_PATH}`")
        # spark.sql(f"VACUUM delta.`{DELTA_TABLE_PATH}` RETAIN 168 HOURS")
        print("✅ Portfolio Delta table optimized.")
    except Exception as e:
        print(f"⚠️ Error optimizing Delta tables: {e}")

# Call optimization function
optimize_portfolio_delta()
