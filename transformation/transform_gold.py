#building schema

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

#Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [GOLD] %(message)s"
)
log = logging.getLogger(__name__)

# Paths
SILVER_PATH = os.path.join("data", "silver", "hs84_silver.parquet")
GOLD_DIR    = os.path.join("data", "gold")


def create_spark_session():
    return SparkSession.builder \
        .appName("EPC_Procurement_Gold") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()


def run():
    log.info("Starting Gold transformation...")
    os.makedirs(GOLD_DIR, exist_ok=True)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Reading Silver path
    log.info(f"Reading silver: {SILVER_PATH}")
    df = spark.read.parquet(SILVER_PATH)
    log.info(f"Silver rows: {df.count()}")

    # dim_country (star schema)
    log.info("Building dim_country...")
    dim_country = df.select("country_or_area") \
                    .distinct() \
                    .orderBy("country_or_area")

    #surrogate key
    window_country = Window.orderBy("country_or_area")
    dim_country = dim_country.withColumn(
        "country_id", F.row_number().over(window_country)
    ).select("country_id", "country_or_area")

    dim_country.write.mode("overwrite").parquet(
        os.path.join(GOLD_DIR, "dim_country.parquet")
    )
    log.info(f"dim_country saved → {dim_country.count()} countries")

    # dim_material
    log.info("Building dim_material...")
    dim_material = df.select("commodity", "material_category") \
                     .distinct() \
                     .orderBy("material_category", "commodity")

    window_material = Window.orderBy("material_category", "commodity")
    dim_material = dim_material.withColumn(
        "material_id", F.row_number().over(window_material)
    ).select("material_id", "commodity", "material_category")

    dim_material.write.mode("overwrite").parquet(
        os.path.join(GOLD_DIR, "dim_material.parquet")
    )
    log.info(f"dim_material saved → {dim_material.count()} materials")

    # dim_date
    log.info("Building dim_date...")
    dim_date = df.select("year").distinct().orderBy("year")

    window_date = Window.orderBy("year")
    dim_date = dim_date.withColumn(
        "date_id", F.row_number().over(window_date)
    ).select("date_id", "year")

    dim_date.write.mode("overwrite").parquet(
        os.path.join(GOLD_DIR, "dim_date.parquet")
    )
    log.info(f"dim_date saved → {dim_date.count()} years")

    # fact_trade_flows
    log.info("Building fact_trade_flows...")

    # joining with dimensions to get surrogate keys
    fact = df.join(dim_country,  on="country_or_area", how="left") \
             .join(dim_material, on=["commodity", "material_category"], how="left") \
             .join(dim_date,     on="year", how="left")

    # selecting only needed columns for fact table
    fact = fact.select(
        "country_id",
        "material_id",
        "date_id",
        "year",
        "country_or_area",
        "material_category",
        "commodity",
        "trade_usd",
        "trade_usd_millions",
        "weight_kg",
        "quantity",
        "quantity_name"
    )

    #commodity < 3 countries in a year?, add supply risk flag
    supplier_count = fact.groupBy("commodity", "year") \
                         .agg(F.countDistinct("country_or_area")
                               .alias("supplier_count"))

    fact = fact.join(supplier_count, on=["commodity", "year"], how="left")
    fact = fact.withColumn(
        "supply_risk_flag",
        F.when(F.col("supplier_count") < 3, "HIGH RISK").otherwise("NO RISK")
    )

    fact.write.mode("overwrite").parquet(
        os.path.join(GOLD_DIR, "fact_trade_flows.parquet")
    )
    log.info(f"fact_trade_flows saved → {fact.count()} rows")

    # Preview
    log.info("Sample fact_trade_flows:")
    fact.select(
        "country_or_area", "year", "material_category",
        "trade_usd_millions", "supply_risk_flag"
    ).show(5, truncate=False)

    spark.stop()
    log.info("Gold transformation complete.")


if __name__ == "__main__":
    run()