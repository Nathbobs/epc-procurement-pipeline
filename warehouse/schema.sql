
-- EPC Procurement Intelligence Pipeline (Schema for gold layer in Supabase PostgreSQL)

-- Drop tables if they exist
DROP TABLE IF EXISTS fact_trade_flows CASCADE;
DROP TABLE IF EXISTS dim_country CASCADE;
DROP TABLE IF EXISTS dim_material CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

-- Dimension: Country
CREATE TABLE dim_country (
    country_id      SERIAL PRIMARY KEY,
    country_or_area TEXT NOT NULL UNIQUE
);

-- Dimension: Material
CREATE TABLE dim_material (
    material_id       SERIAL PRIMARY KEY,
    commodity         TEXT NOT NULL,
    material_category TEXT NOT NULL
);

-- Dimension: Date
CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    year    INTEGER NOT NULL UNIQUE
);

-- Fact: Trade Flows
CREATE TABLE fact_trade_flows (
    id                  SERIAL PRIMARY KEY,
    country_id          INTEGER REFERENCES dim_country(country_id),
    material_id         INTEGER REFERENCES dim_material(material_id),
    date_id             INTEGER REFERENCES dim_date(date_id),
    year                INTEGER,
    country_or_area     TEXT,
    material_category   TEXT,
    commodity           TEXT,
    trade_usd           DOUBLE PRECISION,
    trade_usd_millions  DOUBLE PRECISION,
    weight_kg           DOUBLE PRECISION,
    quantity            DOUBLE PRECISION,
    quantity_name       TEXT,
    supplier_count      INTEGER,
    supply_risk_flag    TEXT
);

-- Indexes for dashboard query performance 
CREATE INDEX idx_fact_year             ON fact_trade_flows(year);
CREATE INDEX idx_fact_material_cat     ON fact_trade_flows(material_category);
CREATE INDEX idx_fact_country          ON fact_trade_flows(country_or_area);
CREATE INDEX idx_fact_supply_risk      ON fact_trade_flows(supply_risk_flag);