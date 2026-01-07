# Urban Analytics Platform - Microsoft Fabric Project

## Overview
A unified analytics platform built on Microsoft Fabric that integrates:
- **Mobility Data** - NYC Taxi trip records (2015-2024)
- **Environmental Data** - OpenAQ air quality measurements
- **Economic Data** - World Bank GDP & ECB FX rates

## Architecture
This project follows the **Medallion Architecture** pattern:
- **Bronze** → Raw data ingestion
- **Silver** → Cleaned and transformed data
- **Gold** → Star schema for analytics

## Project Components

### Data Ingestion (Bronze Layer)
| Component | Type | Description |
|-----------|------|-------------|
| pl_ingest_yellow_taxi_bronze | Pipeline | Ingests yellow taxi parquet files |
| pl_ingest_green_taxi_bronze | Pipeline | Ingests green taxi parquet files |
| nb_ingest_openaq_bronze | Notebook | Ingests air quality data from OpenAQ API |
| df_ingest_worldbank_bronze | Dataflow | Ingests GDP data from World Bank API |
| df_ingest_ecb_bronze | Dataflow | Ingests FX rates from ECB API |
| df_ingest_taxi_zone_lookup | Dataflow | Ingests NYC taxi zone lookup data |

### Data Transformation (Silver Layer)
| Component | Type | Description |
|-----------|------|-------------|
| nb_transform_taxi | Notebook | Cleans and standardizes taxi data |
| nb_transform_openaq | Notebook | Cleans air quality data |
| nb_transform_economic | Notebook | Transforms GDP and FX data |

### Data Modeling (Gold Layer)
| Table | Type | Description |
|-------|------|-------------|
| FactTaxiDaily | Fact | Daily aggregated taxi trips |
| FactAirQualityDaily | Fact | Daily air quality measurements |
| DimDate | Dimension | Date dimension with calendar attributes |
| DimZone | Dimension | NYC taxi zones |
| DimFX | Dimension | USD/EUR exchange rates |
| DimGDP | Dimension | GDP by country and year |

### Visualization
| Component | Type | Description |
|-----------|------|-------------|
| nb_visualizations_gold | Notebook | 4 analytical dashboards |

## Dashboards Created
1. **Mobility Dashboard** - Trip volumes, fares, busiest zones
2. **Air Quality Dashboard** - Pollution trends, seasonal patterns
3. **Correlation Dashboard** - Mobility vs Air Quality analysis
4. **Economic Dashboard** - Revenue in USD/EUR, GDP context

## Key Analytical Questions Answered
1. How does traffic intensity relate to air quality in NYC?
2. Which zones/times show strongest taxi demand?
3. What is average revenue in USD vs EUR?
4. How do mobility/economic trends compare over time?

## Technologies Used
- Microsoft Fabric (Lakehouse, Warehouse, Data Factory, Notebooks)
- PySpark
- Python (pandas, matplotlib, seaborn, plotly)
- Delta Lake

## Author
**[Your Full Name]**  
Data Engineering Intern  
[Your Contact Info]