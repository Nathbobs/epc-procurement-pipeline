import pandas as pd
import os
import logging
from datetime import datetime

# Logging setup 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [INGEST] %(message)s"
)
log = logging.getLogger(__name__)

# Paths
RAW_CSV    = os.path.join("data", "raw", "hs84_trade_raw.csv")
BRONZE_DIR = os.path.join("data", "bronze")

# O&G relevant commodity keywords
OIL_GAS_KEYWORDS = [
    "pump", "compressor", "valve", "boiler", "turbine",
    "engine", "reactor", "heat exchanger", "separator",
    "pipe", "filter", "centrifuge", "drilling"
]

def run():
    log.info("Starting ingestion...")
    os.makedirs(BRONZE_DIR, exist_ok=True)

    # Load raw CSV
    log.info(f"Reading CSV: {RAW_CSV}")
    df = pd.read_csv(RAW_CSV)
    log.info(f"Raw shape: {df.shape}")

    # Rename columns to snake_case
    df.columns = [
        "country_or_area", "year", "commodity",
        "flow", "trade_usd", "weight_kg",
        "quantity_name", "quantity"
    ]

    # Filter: Imports only
    df = df[df["flow"] == "Import"].copy()
    log.info(f"After Import filter: {df.shape}")

    # Filter: O&G relevant commodities only
    pattern = "|".join(OIL_GAS_KEYWORDS)
    df = df[df["commodity"].str.lower().str.contains(pattern, na=False)].copy()
    log.info(f"After O&G keyword filter: {df.shape}")

    # Basic type casting
    df["year"]       = pd.to_numeric(df["year"], errors="coerce")
    df["trade_usd"]  = pd.to_numeric(df["trade_usd"], errors="coerce")
    df["weight_kg"]  = pd.to_numeric(df["weight_kg"], errors="coerce")
    df["quantity"]   = pd.to_numeric(df["quantity"], errors="coerce")

    # Add ingestion timestamp
    df["ingested_at"] = datetime.utcnow().isoformat()

    # 7. Save as parquet (Bronze layer)
    output_path = os.path.join(BRONZE_DIR, "hs84_bronze.parquet")
    df.to_parquet(output_path, index=False)
    log.info(f"Bronze layer saved → {output_path}")
    log.info(f"Final shape: {df.shape}")

    return df

if __name__ == "__main__":
    run()