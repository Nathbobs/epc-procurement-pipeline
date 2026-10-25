# EPC Procurement Intelligence Pipeline

> A production-grade data engineering pipeline for Oil & Gas procurement analytics — built on Medallion Architecture using PySpark, Supabase PostgreSQL, and Streamlit.

**Live Dashboard → [nathaniel-epc.streamlit.app](https://nathaniel-epc.streamlit.app)**

---

## What This Project Does

This pipeline ingests UN Comtrade trade data (HS Chapter 84 — industrial machinery) and transforms it into a procurement intelligence dashboard for Oil & Gas equipment buyers. It answers questions like:

- Which countries supply the most compressors, turbines, and valves?
- How have import values trended since 2010?
- Which commodities have fewer than 3 global suppliers, signalling supply chain risk?

---

## Architecture Overview

```
UN Comtrade CSV
      │
      ▼
┌─────────────┐     pandas      ┌──────────────────┐
│  Raw Layer  │ ─────────────▶ │   Bronze Layer   │  (hs84_bronze.parquet)
│  (CSV file) │                └──────────────────┘
└─────────────┘                         │
                                        │  PySpark
                                        ▼
                               ┌──────────────────┐
                               │   Silver Layer   │  (hs84_silver.parquet)
                               └──────────────────┘
                                        │
                                        │  PySpark
                                        ▼
                               ┌──────────────────────────────────────┐
                               │            Gold Layer                │
                               │  dim_country   dim_material          │
                               │  dim_date      fact_trade_flows      │
                               └──────────────────────────────────────┘
                                        │
                                        │  SQLAlchemy / psycopg2
                                        ▼
                               ┌──────────────────┐
                               │  Supabase (PG)   │  (data warehouse)
                               └──────────────────┘
                                        │
                                        │  Streamlit + Plotly
                                        ▼
                               ┌──────────────────┐
                               │    Dashboard     │
                               └──────────────────┘
```

The pipeline follows the **Medallion (Lakehouse) Architecture** — a layered approach where each layer progressively cleans and enriches data. All intermediate layers are persisted as **Parquet files** for columnar, compressed, schema-preserving storage.

---

## Layer-by-Layer Breakdown

### Raw → Bronze (`ingestion/ingest.py`)

**Tool:** pandas  
**Input:** `data/raw/hs84_trade_raw.csv` (UN Comtrade export)  
**Output:** `data/bronze/hs84_bronze.parquet`

What happens here:
1. Columns are renamed to `snake_case` (8 fields: `country_or_area`, `year`, `commodity`, `flow`, `trade_usd`, `weight_kg`, `quantity_name`, `quantity`)
2. Filtered to **Imports only** — the `flow` column can be "Import" or "Export"; we only care about what countries are buying
3. Keyword filtered to O&G-relevant equipment using a regex OR pattern across 13 terms: `pump`, `compressor`, `valve`, `boiler`, `turbine`, `engine`, `reactor`, `heat exchanger`, `separator`, `pipe`, `filter`, `centrifuge`, `drilling`
4. Numeric coercion with `pd.to_numeric(..., errors="coerce")` — malformed values become `NaN` rather than crashing
5. An `ingested_at` UTC timestamp is appended for lineage tracking
6. Saved as Parquet via PyArrow

**Why pandas at this stage?** The raw file is a single CSV and doesn't need distributed processing. pandas is faster to spin up than Spark for a straightforward read-filter-write operation.

---

### Bronze → Silver (`transformation/transform_silver.py`)

**Tool:** PySpark  
**Input:** `data/bronze/hs84_bronze.parquet`  
**Output:** `data/silver/hs84_silver.parquet`

What happens here:
1. **Null drop** on critical columns: `country_or_area`, `year`, `commodity`, `trade_usd` — rows missing any of these are unanalysable
2. **Type enforcement** via PySpark's `.cast()` — `trade_usd` and `weight_kg` → `DoubleType`, `year` → `IntegerType`
3. **Business rule filter:** rows where `trade_usd <= 0` are dropped (zero-value trade records are data artefacts)
4. **Whitespace trimming** on string columns using `F.trim()`
5. **Feature engineering — `material_category`:** a derived column built using a chained `F.when(...).otherwise(...)` expression that maps raw commodity descriptions to 11 clean categories (Pumps, Compressors, Valves, Boilers, Turbines, Engines, Filters, Heat Exchangers, Separators, Drilling Equipment, Other Machinery). This is implemented in a standalone `classify_material()` function for reusability.
6. **`trade_usd_millions`:** a derived column `trade_usd / 1,000,000` rounded to 4 decimal places — makes dashboard display cleaner
7. **Year range filter:** only records from 2010 onwards are kept for business relevance

**Spark session config:** `spark.driver.memory` set to `2g` to handle the dataset without memory errors on a local machine.

---

### Silver → Gold (`transformation/transform_gold.py`)

**Tool:** PySpark  
**Input:** `data/silver/hs84_silver.parquet`  
**Output:** 4 Parquet files in `data/gold/`

This stage builds a **Star Schema** — a dimensional model optimised for analytical queries.

#### Dimension Tables

**`dim_country`**
- Distinct `country_or_area` values
- Surrogate key `country_id` generated using `F.row_number().over(Window.orderBy("country_or_area"))`
- Ordered alphabetically for deterministic key assignment

**`dim_material`**
- Distinct `(commodity, material_category)` pairs
- Surrogate key `material_id` via `Window.orderBy("material_category", "commodity")`

**`dim_date`**
- Distinct `year` values
- Surrogate key `date_id` via `Window.orderBy("year")`

#### Fact Table — `fact_trade_flows`

Built by joining the Silver layer with all three dimensions using left joins on natural keys. Contains:

| Column | Description |
|--------|-------------|
| `country_id`, `material_id`, `date_id` | Surrogate FKs to dimensions |
| `country_or_area`, `commodity`, `year` | Degenerate dimensions (kept for query convenience) |
| `trade_usd`, `trade_usd_millions` | Measures |
| `weight_kg`, `quantity`, `quantity_name` | Additional measures |
| `material_category` | Denormalised for dashboard filter performance |
| `supplier_count` | Computed metric (see below) |
| `supply_risk_flag` | Derived risk classification |

**Supply Risk Logic:**  
A `supplier_count` is computed by grouping on `(commodity, year)` and running `countDistinct("country_or_area")`. This is then joined back to the fact table. If `supplier_count < 3`, the record is flagged `"HIGH RISK"`; otherwise `"NO RISK"`. This flags commodities with narrow supplier bases — a key procurement risk indicator.

---

### Data Quality Checks (`quality/run_checks.py`)

**Tool:** pandas (reads from Gold Parquet)  
**6 checks run against `fact_trade_flows`:**

| Check | Rule |
|-------|------|
| `trade_usd` not null | Zero nulls allowed |
| `trade_usd > 0` | No zero or negative trade values |
| `year` in range | Must be between 2010 and 2024 |
| `material_category` valid | Must be one of the 11 defined categories |
| `country_or_area` not null | No orphaned country records |
| `supply_risk_flag` valid | Must be `"HIGH RISK"` or `"NO RISK"` |

Each check calls a `check(name, passed, details)` helper that logs `PASSED` or `FAILED` with a count of offending rows. The pipeline treats quality check failures as non-blocking (the orchestrator only hard-aborts on upstream stage failures).

---

### Pipeline Orchestrator (`run_pipeline.py`)

Executes stages in order using `importlib.util` to dynamically load and run each module:

```
1. Ingestion      (Raw → Bronze)
2. Transformation (Bronze → Silver)
3. Transformation (Silver → Gold)
4. Warehouse      (Gold → Supabase)
5. Quality Checks (Gold layer)
```

Stages 1–4 are abort-on-failure (a Bronze failure means Silver cannot run). Quality checks (stage 5) are run regardless. Each stage's elapsed time is logged.

---

### Warehouse Load

**Supabase** is a managed cloud service that provides a hosted **PostgreSQL** database as its core engine, plus a web UI, auto-generated REST API, and auth on top. Critically, the underlying database is plain Postgres — there is nothing Supabase-specific in the code. If you swapped Supabase for AWS RDS or a self-hosted Postgres instance, nothing in the pipeline would need to change.

**SQLAlchemy** is a Python database abstraction library that sits between the Python code and the actual database. It is used in two places:

1. **Writing Gold data to Supabase:**
```python
engine = create_engine(DATABASE_URL)
df.to_sql("fact_trade_flows", engine, if_exists="replace")
```
pandas' `.to_sql()` requires a SQLAlchemy engine to know what database driver to use and how to format queries.

2. **Reading from Supabase in the dashboard:**
```python
fact = pd.read_sql("SELECT * FROM fact_trade_flows", engine)
```
Same engine in reverse — `pd.read_sql()` uses it to run a query and return a DataFrame.

SQLAlchemy wraps `psycopg2` (the raw PostgreSQL driver that handles the actual network connection) and adds a consistent API, connection pooling, and the engine abstraction that pandas expects. Both `sqlalchemy` and `psycopg2-binary` are in `requirements.txt` — you need both; SQLAlchemy uses psycopg2 under the hood.

The `DATABASE_URL` is a standard `postgresql://user:pass@host/db` connection string, read from a `.env` file via `python-dotenv`.

---

## Dashboard (`dashboard/app.py`)

**Stack:** Streamlit + Plotly + SQLAlchemy

### Data Loading
```python
@st.cache_data(ttl=300)
def load_data():
    engine = create_engine(DATABASE_URL)
    fact    = pd.read_sql("SELECT * FROM fact_trade_flows", engine)
    ...
```
Data is cached for 300 seconds (5 minutes) to avoid hitting the database on every interaction. `@st.cache_data` serialises the DataFrames and reuses them across reruns within the TTL window.

### Navigation
Client-side routing is implemented using `st.session_state` — clicking a sidebar button sets `st.session_state.page` and calls `st.rerun()`. The active page is rendered as a styled HTML block (not a Streamlit button) to show a visual active state, since Streamlit has no native active button styling.

### Pages

**Overview**
- 4 KPI metrics: Total Import Value, Total Records, Supplier Countries, Material Categories
- Horizontal bar chart: Import Value by Material Category
- Donut chart: Category Share (using Plotly `hole=0.4`)

**Trade Trends**
- Interactive filters: category selectbox + year range slider
- Line chart: Total import value by year
- Stacked bar chart: Import value by category over time

**Supplier Analysis**
- Filters: category + year
- Top-10 supplier countries by import value (bar chart)
- Country × Category import value heatmap (`go.Heatmap` with a custom teal colorscale)
- Sortable data table

**Supply Risk**
- KPIs: total commodities, high-risk count, no-risk count, risk rate %
- Risk distribution donut (red/green)
- Avg supplier count per category — bars coloured red if below threshold of 3, with a dashed vertical reference line
- High-risk commodities table, sorted by trade value

### Theming
A custom dark theme is injected via `st.markdown(..., unsafe_allow_html=True)` with inline CSS targeting Streamlit's internal `data-testid` selectors. The colour palette uses a teal (`#00d4ff`) / dark navy (`#0b0e1a`, `#111827`) scheme inspired by industrial dashboards.

---

## Visitor Tracking (`dashboard/tracker.py`)

On each unique session load, `log_visitor()` runs and:
1. Extracts the client IP from the `X-Forwarded-For` header (Streamlit Cloud sets this)
2. Filters out RFC-1918 private IPs to get the real public IP
3. Enriches with org and city via the **IPInfo API**
4. Authenticates to **Google Sheets** via a service account (`google-auth` + `gspread`)
5. Appends a row: `[timestamp, ip, org, city, user_agent]`

The `_visitor_logged` session state flag prevents duplicate writes on reruns within the same session. Errors are silently swallowed (`except: pass`) so tracking failures never break the dashboard.

---

## CI/CD — Keep-Alive Workflow (`.github/workflows/keep_alive.yml`)

Streamlit Community Cloud hibernates inactive apps. A GitHub Actions cron job pings the live URL every 11 hours:

```yaml
on:
  schedule:
    - cron: "0 */11 * * *"
```

The step runs `curl -s -o /dev/null -w "%{http_code}"` — discards the body, prints the HTTP status code for the log, and keeps the app warm.

---

## Data Source

**UN Comtrade — HS Chapter 84** (Nuclear reactors, boilers, machinery, mechanical appliances)

- Accessed via the UN Comtrade bulk download / API
- Filtered in-pipeline to Import flows only and to O&G-relevant commodity descriptions
- Year coverage: 2010–2024

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Python, pandas |
| Transformation | PySpark (local mode) |
| Storage | Parquet (PyArrow) |
| Warehouse | Supabase (PostgreSQL) via SQLAlchemy |
| Quality | pandas |
| Dashboard | Streamlit, Plotly |
| Visitor Tracking | IPInfo API, Google Sheets API, gspread |
| CI | GitHub Actions |

---

## Key Design Decisions

**Why Medallion Architecture?**  
Each layer is independently rerunnable. If a business rule changes (e.g. the `material_category` mapping), you re-run Silver and Gold without re-ingesting. The Bronze layer preserves the raw data exactly as received.

**Why PySpark for Silver/Gold but pandas for Bronze/Quality?**  
Bronze is a simple filter operation on a flat file — Spark startup overhead (~10s) is not justified. Silver and Gold do columnar transformations and window functions where Spark's execution model is more appropriate. Quality checks read the final Parquet into memory and do simple aggregations — pandas is faster for this.

**Why surrogate keys via `row_number()` over a Window?**  
Avoids a shuffle-heavy `monotonically_increasing_id()` and produces deterministic, human-readable integer keys that are stable across reruns (given the same data) because the `orderBy` is on a natural key.

**Why star schema instead of a flat table?**  
Enables efficient slicing by dimension in SQL (e.g. filter by `country_id` without a full string scan). Also mirrors what a data warehouse team would expect — demonstrates dimensional modelling awareness.

**Why `supply_risk_flag` on the fact table and not a separate dim?**  
It's a measure derived from a per-`(commodity, year)` aggregation — it varies by time period, not just by commodity. Putting it in a dimension would require a slowly-changing dimension (SCD) pattern. Keeping it on the fact is simpler and correct.
