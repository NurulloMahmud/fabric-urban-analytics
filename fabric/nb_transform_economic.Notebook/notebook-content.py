# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1575b5e8-95e6-4f43-84e6-99ca5ca51d40",
# META       "default_lakehouse_name": "lakehouse_bronze_silver",
# META       "default_lakehouse_workspace_id": "6484574c-17ac-4a0b-8124-3be53dd6f724",
# META       "known_lakehouses": [
# META         {
# META           "id": "1575b5e8-95e6-4f43-84e6-99ca5ca51d40"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofmonth

spark = SparkSession.builder.getOrCreate()

BRONZE_TABLE_GDP = "bronze_gdp"
BRONZE_TABLE_FX = "bronze_fx"
SILVER_TABLE_GDP = "silver_gdp"
SILVER_TABLE_FX = "silver_fx_rates"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Load Bronze Data**

# CELL ********************

df_gdp = spark.table(BRONZE_TABLE_GDP)
df_fx = spark.table(BRONZE_TABLE_FX)

print(f"GDP records: {df_gdp.count()}")
print(f"FX records: {df_fx.count()}")

print("\nGDP schema:")
df_gdp.printSchema()

print("\nFX schema:")
df_fx.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Transform and Save**

# CELL ********************

df_silver_gdp = df_gdp.select(
    col("countryiso3code").alias("country_code"),
    col("date").cast("int").alias("year"),
    col("value").alias("gdp_usd")
).dropDuplicates()

df_silver_fx = df_fx.select(
    to_date(col("TIME_PERIOD")).alias("date"),
    col("OBS_VALUE").cast("double").alias("usd_eur_rate"),
    col("CURRENCY").alias("currency_from"),
    col("CURRENCY_DENOM").alias("currency_to")
).withColumn(
    "fx_year", year(col("date"))
).withColumn(
    "fx_month", month(col("date"))
).withColumn(
    "fx_day", dayofmonth(col("date"))
).dropDuplicates()

print(f"Silver GDP records: {df_silver_gdp.count()}")
df_silver_gdp.show(5)

print(f"Silver FX records: {df_silver_fx.count()}")
df_silver_fx.show(5)

df_silver_gdp.write.mode("overwrite").format("delta").saveAsTable(SILVER_TABLE_GDP)
print(f"Saved: {SILVER_TABLE_GDP}")

df_silver_fx.write.mode("overwrite").format("delta").saveAsTable(SILVER_TABLE_FX)
print(f"Saved: {SILVER_TABLE_FX}")

print("\nEconomic Silver transformation complete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
