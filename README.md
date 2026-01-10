# 🏙️ Urban Analytics Platform - Microsoft Fabric Data Engineering Project

A unified analytics platform built on **Microsoft Fabric** that integrates urban mobility data (NYC Taxi), environmental data (Air Quality), and macro-economic indicators (GDP & FX rates) to explore how these domains intersect and influence each other.

![Microsoft Fabric](https://img.shields.io/badge/Microsoft%20Fabric-Data%20Engineering-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.x-orange)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Enabled-green)
![License](https://img.shields.io/badge/License-MIT-yellow)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Project Structure](#project-structure)
- [Data Pipeline Details](#data-pipeline-details)
  - [Bronze Layer (Raw Ingestion)](#bronze-layer-raw-ingestion)
  - [Silver Layer (Transformation)](#silver-layer-transformation)
  - [Gold Layer (Star Schema)](#gold-layer-star-schema)
- [Key Metrics & Statistics](#key-metrics--statistics)
- [Analytical Dashboards](#analytical-dashboards)
- [Technologies Used](#technologies-used)
- [Setup & Deployment](#setup--deployment)
- [Key Analytical Questions](#key-analytical-questions)
- [Author](#author)

---

## Overview

This project demonstrates a complete **end-to-end data engineering solution** using Microsoft Fabric's unified analytics platform. It implements the **Medallion Architecture** (Bronze → Silver → Gold) to process and analyze:

- **808+ million** NYC taxi trip records (2015-2024)
- **40,000+** air quality measurements from 21 NYC-area monitoring stations
- **10 years** of GDP data for 7 major economies
- **6,900+** daily USD/EUR exchange rates

The platform enables cross-domain analytics to answer questions like:
- How does traffic intensity relate to air quality in NYC?
- What is the economic impact of taxi services when viewed through currency fluctuations?
- Which zones and times show the strongest correlation between mobility and pollution?

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES                                        │
├─────────────────┬─────────────────┬─────────────────┬───────────────────────────┤
│   NYC TLC       │    OpenAQ       │   World Bank    │         ECB               │
│  (Parquet)      │    (REST API)   │   (JSON API)    │      (CSV API)            │
└────────┬────────┴────────┬────────┴────────┬────────┴────────────┬──────────────┘
         │                 │                 │                     │
         ▼                 ▼                 ▼                     ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           BRONZE LAYER (Raw Data)                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │ taxi_yellow │  │  openaq_    │  │  bronze_    │  │  bronze_    │             │
│  │ taxi_green  │  │measurements │  │    gdp      │  │    fx       │             │
│  │ (Parquet)   │  │  (Delta)    │  │  (Delta)    │  │  (Delta)    │             │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘             │
│         │                 │                 │                     │              │
│         └─────────────────┴─────────────────┴─────────────────────┘              │
│                                     │                                            │
│                        Fabric Lakehouse (OneLake)                                │
└─────────────────────────────────────┬───────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          SILVER LAYER (Cleaned & Enriched)                       │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────────────┐   │
│  │ silver_taxi_trips│  │silver_air_quality│  │ silver_gdp │ silver_fx_rates │   │
│  │   (808M rows)    │  │   (40K rows)     │  │ (70 rows)  │   (7K rows)     │   │
│  └──────────────────┘  └──────────────────┘  └──────────────────────────────┘   │
│                                                                                  │
│                        Fabric Lakehouse (Delta Tables)                           │
└─────────────────────────────────────┬───────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           GOLD LAYER (Star Schema)                               │
│                                                                                  │
│    ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐  │
│    │  DimDate    │     │  DimZone    │     │   DimFX     │     │  DimGDP     │  │
│    └──────┬──────┘     └──────┬──────┘     └──────┬──────┘     └─────────────┘  │
│           │                   │                   │                              │
│           ▼                   ▼                   ▼                              │
│    ┌─────────────────────────────────┐    ┌─────────────────────────────────┐   │
│    │        FactTaxiDaily            │    │     FactAirQualityDaily         │   │
│    │  (Aggregated Trip Metrics)      │    │  (Daily Pollution Metrics)      │   │
│    └─────────────────────────────────┘    └─────────────────────────────────┘   │
│                                                                                  │
│                           Fabric Warehouse                                       │
└─────────────────────────────────────┬───────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              ANALYTICS LAYER                                     │
│  ┌───────────────┐ ┌───────────────┐ ┌───────────────┐ ┌───────────────┐        │
│  │   Mobility    │ │  Air Quality  │ │  Correlation  │ │   Economic    │        │
│  │   Dashboard   │ │   Dashboard   │ │   Dashboard   │ │   Dashboard   │        │
│  └───────────────┘ └───────────────┘ └───────────────┘ └───────────────┘        │
│                                                                                  │
│                    Fabric Notebook (Python Visualizations)                       │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Sources

| Source | Data Type | Format | API/URL | Date Range |
|--------|-----------|--------|---------|------------|
| **NYC TLC** | Taxi Trip Records | Parquet | `https://d37ci6vzurychx.cloudfront.net/trip-data/` | 2015-2024 |
| **NYC TLC** | Taxi Zone Lookup | CSV | `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv` | Static |
| **OpenAQ** | Air Quality Measurements | JSON API | `https://api.openaq.org/v3` | 2015-2024 |
| **World Bank** | GDP Indicators | JSON API | `https://api.worldbank.org/v2/country/{code}/indicator/NY.GDP.MKTP.CD` | 2015-2024 |
| **ECB** | USD/EUR Exchange Rates | CSV API | `https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A` | 2015-2024 |

---

## Project Structure

```
fabric-urban-analytics/
│
├── 📂 Pipelines (Data Ingestion)
│   ├── pl_ingest_yellow_taxi_bronze/    # Yellow taxi parquet ingestion
│   └── pl_ingest_green_taxi_bronze/     # Green taxi parquet ingestion
│
├── 📂 Dataflows (API Ingestion)
│   ├── df_ingest_ecb_bronze/            # ECB exchange rates (CSV API)
│   ├── df_ingest_worldbank_bronze/      # World Bank GDP (JSON API)
│   └── df_ingest_taxi_zone_lookup/      # Taxi zone reference data
│
├── 📂 Notebooks
│   ├── nb_ingest_openaq_bronze/         # OpenAQ API ingestion
│   ├── nb_transform_taxi/               # Taxi data transformation
│   ├── nb_transform_openaq/             # Air quality transformation
│   ├── nb_transform_economic/           # GDP & FX transformation
│   └── nb_visualizations_gold/          # Analytics dashboards
│
├── 📂 Lakehouse
│   └── lakehouse_bronze_silver/         # Bronze & Silver Delta tables
│
├── 📂 Warehouse
│   └── warehouse_gold/                  # Star schema (Facts & Dimensions)
│
└── README.md
```

---

## Data Pipeline Details

### Bronze Layer (Raw Ingestion)

The Bronze layer captures raw data from all sources with minimal transformation.

#### 1. NYC Taxi Data (`pl_ingest_yellow_taxi_bronze`, `pl_ingest_green_taxi_bronze`)

**Technology:** Data Factory Pipeline with ForEach + Copy Activity

**Process:**
- Iterates through monthly parquet files (2015-01 to 2024-10)
- Copies files from NYC TLC CloudFront to Lakehouse Files
- Preserves original parquet format

**Output:** 
- `Files/bronze/taxi_yellow/` - 118 parquet files
- `Files/bronze/taxi_green/` - 118 parquet files

#### 2. Air Quality Data (`nb_ingest_openaq_bronze`)

**Technology:** PySpark Notebook with REST API calls

**Process:**
```python
# Key steps:
1. Fetch all US locations from OpenAQ API
2. Filter locations within NYC bounding box (40.4-41.0°N, 74.3-73.7°W)
3. For each location, fetch daily sensor measurements
4. Collect measurements for PM2.5, NO2, O3, and other pollutants
5. Save as Delta tables
```

**Output:**
- `bronze_openaq_measurements` - 40,056 records
- `bronze_openaq_locations` - 21 NYC-area monitoring stations

#### 3. GDP Data (`df_ingest_worldbank_bronze`)

**Technology:** Dataflow Gen2 with Power Query M

**Process:**
```
1. Fetch JSON from World Bank API for 7 countries (US, GB, DE, FR, JP, CN, IN)
2. Parse and expand JSON records
3. Remove unnecessary columns
4. Combine all country data using Table.Combine()
5. Load to Lakehouse
```

**Output:** `bronze_gdp` - 70 records (7 countries × 10 years)

#### 4. Exchange Rates (`df_ingest_ecb_bronze`)

**Technology:** Dataflow Gen2 with Power Query M

**Process:**
```
1. Fetch CSV from ECB Data API
2. Promote headers and set column types
3. Load to Lakehouse
```

**Output:** `bronze_fx` - 6,972 daily exchange rates

#### 5. Taxi Zone Lookup (`df_ingest_taxi_zone_lookup`)

**Technology:** Dataflow Gen2 with Power Query M

**Source URL:** `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`

**Output:** `bronze_taxi_zone_lookup` - 265 NYC taxi zones

---

### Silver Layer (Transformation)

The Silver layer applies data quality rules, standardization, and enrichment.

#### 1. Taxi Trips (`nb_transform_taxi`)

**Input:** 236 raw parquet files (118 yellow + 118 green)

**Transformations:**
```python
# Schema Standardization
- Unify column names between yellow (tpep_*) and green (lpep_*) taxi schemas
- Cast all columns to consistent types (LONG, DOUBLE, STRING)
- Add taxi_type identifier ('yellow' or 'green')

# Data Quality Filters
- Date range: 2015-01-01 to 2024-12-31
- fare_amount >= 0
- total_amount >= 0
- trip_distance: 0 to 500 miles
- trip_duration: 0 to 180 minutes

# Null Handling
- passenger_count: NULL → 1
- RatecodeID: NULL → 1
- store_and_fwd_flag: NULL → 'N'
- congestion_surcharge: NULL → 0.0
- Airport_fee: NULL → 0.0

# Feature Engineering
- Extract: pickup_year, pickup_month, pickup_day, pickup_hour, pickup_dayofweek
- Calculate: trip_duration_minutes
```

**Output:** `silver_taxi_trips` - **808,608,538 records**

| Taxi Type | Record Count |
|-----------|-------------|
| Yellow | 741,653,098 |
| Green | 66,955,440 |

#### 2. Air Quality (`nb_transform_openaq`)

**Input:** `bronze_openaq_measurements`

**Transformations:**
```python
# Date Extraction
- measurement_date (DATE)
- measurement_year, measurement_month, measurement_day
- measurement_dayofweek

# Data Quality
- Filter: value IS NOT NULL AND value >= 0
- Deduplicate records
```

**Output:** `silver_air_quality` - **39,931 records**

| Year | Records |
|------|---------|
| 2016 | 7,568 |
| 2017 | 11,058 |
| 2018 | 9,617 |
| 2019 | 4,534 |
| 2020 | 1,355 |
| 2021 | 367 |
| 2022 | 278 |
| 2023 | 2,443 |
| 2024 | 2,711 |

#### 3. Economic Data (`nb_transform_economic`)

**GDP Transformation:**
```python
# Input: bronze_gdp
# Output: silver_gdp (70 records)

- Rename: countryiso3code → country_code
- Cast: date → year (INT)
- Rename: value → gdp_usd
```

**FX Transformation:**
```python
# Input: bronze_fx
# Output: silver_fx_rates (6,972 records)

- Rename: TIME_PERIOD → date, OBS_VALUE → usd_eur_rate
- Extract: fx_year, fx_month, fx_day
- Keep: currency_from, currency_to
```

---

### Gold Layer (Star Schema)

The Gold layer implements a dimensional model optimized for analytics.

```
                              ┌─────────────┐
                              │   DimDate   │
                              ├─────────────┤
                              │ date_key PK │
                              │ date        │
                              │ year        │
                              │ quarter     │
                              │ month       │
                              │ day_of_month│
                              │ day_of_week │
                              │ is_weekend  │
                              │ week_of_year│
                              └──────┬──────┘
                                     │
         ┌───────────────────────────┼───────────────────────────┐
         │                           │                           │
         ▼                           ▼                           ▼
┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
│  FactTaxiDaily  │         │     DimFX       │         │FactAirQuality   │
├─────────────────┤         ├─────────────────┤         │     Daily       │
│ date_key FK     │         │ date_key PK     │         ├─────────────────┤
│ pickup_zone FK  │         │ fx_date         │         │ date_key FK     │
│ dropoff_zone FK │         │ usd_eur_rate    │         │ location_id     │
│ taxi_type       │         │ fx_year         │         │ location_name   │
│ trip_count      │         │ fx_month        │         │ parameter_name  │
│ total_passengers│         │ fx_day          │         │ parameter_units │
│ total_distance  │         └─────────────────┘         │ measurement_cnt │
│ total_fare      │                                     │ avg_value       │
│ total_tips      │         ┌─────────────────┐         │ min_value       │
│ total_revenue   │         │    DimGDP       │         │ max_value       │
│ avg_trip_duration│        ├─────────────────┤         └─────────────────┘
└────────┬────────┘         │ country_code PK │
         │                  │ year         PK │
         │                  │ gdp_usd         │
         ▼                  └─────────────────┘
┌─────────────────┐
│    DimZone      │
├─────────────────┤
│ zone_id PK      │
│ borough         │
│ zone_name       │
│ service_zone    │
└─────────────────┘
```

#### Fact Tables

**FactTaxiDaily** - Daily aggregated taxi metrics
```sql
CREATE TABLE FactTaxiDaily AS
SELECT 
    pickup_year AS year,
    pickup_month AS month,
    pickup_day AS day,
    CAST(pickup_year * 10000 + pickup_month * 100 + pickup_day AS INT) AS date_key,
    PULocationID AS pickup_zone_id,
    DOLocationID AS dropoff_zone_id,
    taxi_type,
    COUNT(*) AS trip_count,
    SUM(passenger_count) AS total_passengers,
    ROUND(SUM(trip_distance), 2) AS total_distance,
    ROUND(SUM(fare_amount), 2) AS total_fare,
    ROUND(SUM(tip_amount), 2) AS total_tips,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_trip_duration
FROM silver_taxi_trips
GROUP BY year, month, day, pickup_zone_id, dropoff_zone_id, taxi_type;
```

**FactAirQualityDaily** - Daily pollution metrics
```sql
CREATE TABLE FactAirQualityDaily AS
SELECT 
    measurement_year AS year,
    measurement_month AS month,
    measurement_day AS day,
    CAST(measurement_year * 10000 + measurement_month * 100 + measurement_day AS INT) AS date_key,
    location_id,
    location_name,
    parameter_name,
    parameter_units,
    COUNT(*) AS measurement_count,
    ROUND(AVG(value), 4) AS avg_value,
    ROUND(MIN(value), 4) AS min_value,
    ROUND(MAX(value), 4) AS max_value
FROM silver_air_quality
GROUP BY year, month, day, location_id, location_name, parameter_name, parameter_units;
```

#### Dimension Tables

**DimDate** - Calendar dimension with time intelligence attributes
```sql
CREATE TABLE DimDate AS
WITH all_dates AS (
    SELECT DISTINCT CAST(date AS DATE) AS date_val FROM silver_fx_rates
    UNION
    SELECT DISTINCT CAST(pickup_datetime AS DATE) FROM silver_taxi_trips
    UNION  
    SELECT DISTINCT CAST(measurement_date AS DATE) FROM silver_air_quality
)
SELECT 
    CAST(YEAR(date_val) * 10000 + MONTH(date_val) * 100 + DAY(date_val) AS INT) AS date_key,
    date_val AS date,
    YEAR(date_val) AS year,
    CAST((MONTH(date_val) - 1) / 3 + 1 AS INT) AS quarter,
    MONTH(date_val) AS month,
    DAY(date_val) AS day_of_month,
    DATEPART(weekday, date_val) AS day_of_week,
    CASE WHEN DATEPART(weekday, date_val) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
    DATEPART(week, date_val) AS week_of_year
FROM all_dates;
```

**DimZone** - NYC Taxi zone reference
```sql
CREATE TABLE DimZone AS
SELECT 
    LocationID AS zone_id,
    Borough AS borough,
    Zone AS zone_name,
    service_zone
FROM bronze_taxi_zone_lookup;
```

**DimFX** - Exchange rate dimension
```sql
CREATE TABLE DimFX AS
SELECT 
    CAST(fx_year * 10000 + fx_month * 100 + fx_day AS INT) AS date_key,
    date AS fx_date,
    usd_eur_rate,
    fx_year,
    fx_month,
    fx_day
FROM silver_fx_rates;
```

**DimGDP** - GDP by country and year
```sql
CREATE TABLE DimGDP AS
SELECT 
    country_code,
    year,
    gdp_usd
FROM silver_gdp;
```

---

## Key Metrics & Statistics

### Data Volume Summary

| Layer | Table | Records | Size |
|-------|-------|---------|------|
| Bronze | taxi_yellow (files) | 118 files | ~50 GB |
| Bronze | taxi_green (files) | 118 files | ~5 GB |
| Bronze | openaq_measurements | 40,056 | ~5 MB |
| Bronze | bronze_fx | 6,972 | <1 MB |
| Bronze | bronze_gdp | 70 | <1 MB |
| Silver | silver_taxi_trips | 808,608,538 | ~40 GB |
| Silver | silver_air_quality | 39,931 | ~3 MB |
| Silver | silver_fx_rates | 6,972 | <1 MB |
| Silver | silver_gdp | 70 | <1 MB |
| Gold | FactTaxiDaily | Aggregated | ~500 MB |
| Gold | FactAirQualityDaily | Aggregated | ~2 MB |

### Taxi Trip Distribution by Year

| Year | Trips | % of Total |
|------|-------|------------|
| 2015 | 164,785,570 | 20.4% |
| 2016 | 146,939,275 | 18.2% |
| 2017 | 124,772,955 | 15.4% |
| 2018 | 111,292,285 | 13.8% |
| 2019 | 90,333,919 | 11.2% |
| 2020 | 26,186,922 | 3.2% |
| 2021 | 31,703,959 | 3.9% |
| 2022 | 40,149,805 | 5.0% |
| 2023 | 38,655,124 | 4.8% |
| 2024 | 33,788,724 | 4.2% |

*Note: Significant drop in 2020 due to COVID-19 pandemic*

### Air Quality Monitoring Coverage

- **Total Locations:** 21 stations in NYC metropolitan area
- **Parameters Monitored:** PM2.5, NO2, O3, and others
- **Geographic Coverage:** All 5 boroughs + nearby NJ locations

---

## Analytical Dashboards

The `nb_visualizations_gold` notebook generates four analytical dashboards:

### 1. Mobility Dashboard
- Monthly trip volume trends (2015-2024)
- Average fare per trip over time
- Top 10 busiest pickup zones
- Yellow vs Green taxi comparison

### 2. Air Quality Dashboard
- PM2.5 levels over time
- Top 10 locations by pollution levels
- Seasonal pollution patterns
- Year-over-year comparison

### 3. Correlation Dashboard
- Scatter plot: Daily trips vs air quality
- Dual-axis time series overlay
- Seasonal trip patterns
- Correlation coefficient analysis

### 4. Economic Impact Dashboard
- Revenue in USD vs EUR
- Exchange rate trends
- Yearly revenue with GDP context
- Year-over-year revenue growth

---

## Technologies Used

| Category | Technology |
|----------|------------|
| **Platform** | Microsoft Fabric |
| **Data Lake** | OneLake |
| **Storage Format** | Delta Lake (Parquet) |
| **ETL - Pipelines** | Data Factory Pipelines |
| **ETL - Dataflows** | Dataflow Gen2 (Power Query M) |
| **ETL - Notebooks** | PySpark (Python) |
| **Data Warehouse** | Fabric Warehouse (T-SQL) |
| **Visualization** | Python (matplotlib, seaborn, plotly) |
| **Version Control** | Git / GitHub |

### Key Libraries

```python
# Data Processing
pyspark
pandas

# API Integration
requests

# Visualization
matplotlib
seaborn
plotly
```

---

## Key Analytical Questions

This platform answers the following business questions:

1. **How does traffic intensity (trips per day) relate to air quality (PM2.5/NO2 levels) in NYC?**
   - Correlation analysis between daily trip counts and pollution measurements

2. **Which zones or times of day show the strongest link between taxi demand and pollution peaks?**
   - Zone-level analysis with temporal patterns

3. **What is the average revenue per trip in USD vs EUR, and how does exchange rate fluctuation affect it?**
   - Economic impact analysis with currency conversion

4. **Over multiple years, do we see mobility/economic growth at the expense of environmental quality?**
   - Long-term trend analysis across all three domains
