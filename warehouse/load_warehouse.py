
import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WAREHOUSE] %(message)s"
)
log = logging.getLogger(__name__)

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

GOLD_DIR    = os.path.join("data", "gold")
SCHEMA_FILE = os.path.join("warehouse", "schema.sql")


def get_engine():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not set in .env file")
    return create_engine(DATABASE_URL)


def run_schema(engine):
    #creating table in supabase
    log.info("Running schema.sql...")
    with open(SCHEMA_FILE, "r") as f:
        sql = f.read()
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    log.info("Schema created successfully.")


def load_table(engine, parquet_path, table_name, columns):
    #Loading parquet file into a Supabase table
    log.info(f"Loading {table_name}...")
    df = pd.read_parquet(parquet_path)
    df = df[columns]  # select only relevant columns
    df.to_sql(table_name, engine, if_exists="append", index=False)
    log.info(f"{table_name} loaded → {len(df)} rows")


def run():
    log.info("Starting warehouse load...")
    engine = get_engine()

    
    run_schema(engine) # Create schema

    
    load_table( #Load dim_country
        engine,
        os.path.join(GOLD_DIR, "dim_country.parquet"),
        "dim_country",
        ["country_id", "country_or_area"]
    )

    
    load_table( #Load dim_material
        engine,
        os.path.join(GOLD_DIR, "dim_material.parquet"),
        "dim_material",
        ["material_id", "commodity", "material_category"]
    )

    
    load_table( #Load dim_date
        engine,
        os.path.join(GOLD_DIR, "dim_date.parquet"),
        "dim_date",
        ["date_id", "year"]
    )

    
    load_table( #Load fact_trade_flows
        engine,
        os.path.join(GOLD_DIR, "fact_trade_flows.parquet"),
        "fact_trade_flows",
        [
            "country_id", "material_id", "date_id",
            "year", "country_or_area", "material_category",
            "commodity", "trade_usd", "trade_usd_millions",
            "weight_kg", "quantity", "quantity_name",
            "supplier_count", "supply_risk_flag"
        ]
    )

    log.info("All tables loaded successfully.")
    log.info("Warehouse load complete.")


if __name__ == "__main__":
    run()