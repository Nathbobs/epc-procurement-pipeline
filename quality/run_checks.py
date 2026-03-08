"""
Data quality check: Run validation checks on the Gold layer using Pandas.

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

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [QUALITY] %(message)s"
)
log = logging.getLogger(__name__)

GOLD_PATH = os.path.join("data", "gold", "fact_trade_flows.parquet")

#material categories
VALID_CATEGORIES = {
    "Pumps", "Compressors", "Valves", "Boilers",
    "Turbines", "Engines", "Filters",
    "Heat Exchangers", "Separators",
    "Drilling Equipment", "Other Machinery"
}


def check(name, passed, details=""):
    status = "PASSED" if passed else "FAILED"
    msg = f"  {status}  →  {name}"
    if details:
        msg += f" ({details})"
    log.info(msg)
    return passed


def run():
    log.info("Starting data quality checks...")

    # Load gold fact table
    df = pd.read_parquet(GOLD_PATH)
    log.info(f"Loaded fact_trade_flows: {df.shape}")

    results = []

    #checks
    # trade_usd not null
    null_count = df["trade_usd"].isnull().sum()
    results.append(check(
        "trade_usd not null",
        null_count == 0,
        f"{null_count} nulls found"
    ))

    # trade_usd > 0
    invalid = (df["trade_usd"] <= 0).sum()
    results.append(check(
        "trade_usd > 0",
        invalid == 0,
        f"{invalid} zero/negative values"
    ))

    #year between 2010 and 2024
    out_of_range = (~df["year"].between(2010, 2024)).sum()
    results.append(check(
        "year between 2010-2024",
        out_of_range == 0,
        f"{out_of_range} out-of-range years"
    ))

    # material_category in valid set
    invalid_cats = (~df["material_category"].isin(VALID_CATEGORIES)).sum()
    results.append(check(
        "material_category valid",
        invalid_cats == 0,
        f"{invalid_cats} unknown categories"
    ))

    # country_or_area not null
    null_countries = df["country_or_area"].isnull().sum()
    results.append(check(
        "country_or_area not null",
        null_countries == 0,
        f"{null_countries} nulls found"
    ))

    # supply_risk_flag valid values
    invalid_flags = (~df["supply_risk_flag"].isin(["HIGH RISK", "NO RISK"])).sum()
    results.append(check(
        "supply_risk_flag valid values",
        invalid_flags == 0,
        f"{invalid_flags} invalid flags"
    ))

    # terminal sumary
    log.info("=" * 50)
    log.info("DATA QUALITY REPORT")
    log.info("=" * 50)

    all_passed = all(results)
    if all_passed:
        log.info("ALL CHECKS PASSED — data is production ready.")
    else:
        log.warning("SOME CHECKS FAILED — review data before loading.")
    log.info("=" * 50)

    return all_passed


if __name__ == "__main__":
    run()