"""
Data quality check: Run validation checks on the Gold layer using Great Expectations

Checks:
    1. trade_usd must not be null
    2. trade_usd must be > 0
    3. year must be between 2010 and 2024
    4. material_category must be in known set
    5. country_or_area must not be null
    6. supply_risk_flag must be either 'HIGH RISK' or 'NO RISK'
"""

import os
import logging
import pandas as pd
import great_expectations as ge

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [QUALITY] %(message)s"
)
log = logging.getLogger(__name__)

GOLD_PATH = os.path.join("data", "gold", "fact_trade_flows.parquet")

#material categories
VALID_CATEGORIES = [
    "Pumps", "Compressors", "Valves", "Boilers",
    "Turbines", "Engines", "Filters",
    "Heat Exchangers", "Separators",
    "Drilling Equipment", "Other Machinery"
]


def run():
    log.info("Starting data quality checks...")

    # Load gold fact table with great expectations 
    df = pd.read_parquet(GOLD_PATH)
    log.info(f"Loaded fact_trade_flows: {df.shape}")

    gdf = ge.from_pandas(df)

    results = []
#checks
    # trade_usd not null
    r1 = gdf.expect_column_values_to_not_be_null("trade_usd")
    results.append(("trade_usd not null", r1["success"]))

    # trade_usd > 0 
    r2 = gdf.expect_column_values_to_be_between(
        "trade_usd", min_value=0, strict_min=True
    )
    results.append(("trade_usd > 0", r2["success"]))

    #year between 2010 and 2024
    r3 = gdf.expect_column_values_to_be_between(
        "year", min_value=2010, max_value=2024
    )
    results.append(("year between 2010-2024", r3["success"]))

    # material_category in valid set
    r4 = gdf.expect_column_values_to_be_in_set(
        "material_category", VALID_CATEGORIES
    )
    results.append(("material_category valid", r4["success"]))

    # country_or_area not null
    r5 = gdf.expect_column_values_to_not_be_null("country_or_area")
    results.append(("country_or_area not null", r5["success"]))

    # supply_risk_flag valid values
    r6 = gdf.expect_column_values_to_be_in_set(
        "supply_risk_flag", ["HIGH RISK", "NO RISK"]
    )
    results.append(("supply_risk_flag valid values", r6["success"]))

    # terminal sumary
    log.info("=" * 50)
    log.info("DATA QUALITY REPORT")
    log.info("=" * 50)

    all_passed = True
    for check_name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        log.info(f"  {status}  →  {check_name}")
        if not passed:
            all_passed = False

    log.info("=" * 50)
    if all_passed:
        log.info("ALL CHECKS PASSED — data is production ready.")
    else:
        log.warning("SOME CHECKS FAILED — review data before loading.")
    log.info("=" * 50)

    return all_passed


if __name__ == "__main__":
    run()