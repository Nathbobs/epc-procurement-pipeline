#refine data better using pyspark and save to silver layer.

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [TRANSFORM] %(message)s"
)
log = logging.getLogger(__name__)

# Paths
BRONZE_PATH = os.path.join("data", "bronze", "hs84_bronze.parquet")
SILVER_DIR  = os.path.join("data", "silver")
SILVER_PATH = os.path.join(SILVER_DIR, "hs84_silver.parquet")

def create_spark_session():
    return SparkSession.builder \
        .appName("EPC_Procurement_Silver") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

def classify_material(commodity_col):

    # Map commodity description to a clean material category.
    return (
        F.when(F.lower(commodity_col).contains("pump"),        "Pumps")
         .when(F.lower(commodity_col).contains("compressor"),  "Compressors")
         .when(F.lower(commodity_col).contains("valve"),       "Valves")
         .when(F.lower(commodity_col).contains("boiler"),      "Boilers")
         .when(F.lower(commodity_col).contains("turbine"),     "Turbines")
         .when(F.lower(commodity_col).contains("engine"),      "Engines")
         .when(F.lower(commodity_col).contains("filter"),      "Filters")
         .when(F.lower(commodity_col).contains("heat exchanger"), "Heat Exchangers")
         .when(F.lower(commodity_col).contains("separator"),   "Separators")
         .when(F.lower(commodity_col).contains("drilling"),    "Drilling Equipment")
         .otherwise("Other Machinery")
    )

def run():
    log.info("Starting Silver transformation...")
    os.makedirs(SILVER_DIR, exist_ok=True)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Reading Bronze layer
    log.info(f"Reading bronze: {BRONZE_PATH}")
    df = spark.read.parquet(BRONZE_PATH)
    log.info(f"Bronze shape: ({df.count()}, {len(df.columns)})")

    # Drop rows with null values
    critical_cols = ["country_or_area", "year", "commodity", "trade_usd"]
    df = df.dropna(subset=critical_cols)
    log.info(f"After null drop: {df.count()} rows")

    # cast types
    df = df.withColumn("trade_usd", F.col("trade_usd").cast(DoubleType())) \
           .withColumn("weight_kg", F.col("weight_kg").cast(DoubleType())) \
           .withColumn("quantity",  F.col("quantity").cast(DoubleType())) \
           .withColumn("year",      F.col("year").cast(IntegerType()))

    # Remove rows where trade_usd <= 0
    df = df.filter(F.col("trade_usd") > 0)
    log.info(f"After trade_usd > 0 filter: {df.count()} rows")

    #trim whitespace
    df = df.withColumn("country_or_area", F.trim(F.col("country_or_area")))
    df = df.withColumn("commodity",       F.trim(F.col("commodity")))

    # Adding new material_category column
    df = df.withColumn("material_category", classify_material(F.col("commodity")))

    # adding trade_usd_millions
    df = df.withColumn("trade_usd_millions",
                       F.round(F.col("trade_usd") / 1_000_000, 4))

    # Filtering year range: 2010 onwards for relevance
    df = df.filter(F.col("year") >= 2010)
    log.info(f"After year >= 2010 filter: {df.count()} rows")

    # Save Silver layer
    df.write.mode("overwrite").parquet(SILVER_PATH)
    log.info(f"Silver layer saved → {SILVER_PATH}")

    # Preview
    log.info("Sample silver data:")
    df.select(
        "country_or_area", "year", "material_category",
        "trade_usd_millions", "weight_kg"
    ).show(5, truncate=False)

    spark.stop()
    log.info("Silver transformation complete.")

if __name__ == "__main__":
    run()