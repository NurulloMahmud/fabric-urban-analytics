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
from pyspark.sql.functions import (
    col, year, month, dayofmonth, hour, dayofweek,
    to_timestamp, to_date
)

spark = SparkSession.builder.getOrCreate()

BRONZE_TABLE_MEASUREMENTS = "bronze_openaq_measurements"
BRONZE_TABLE_LOCATIONS = "bronze_openaq_locations"
SILVER_TABLE_MEASUREMENTS = "silver_air_quality"
SILVER_TABLE_LOCATIONS = "silver_air_quality_locations"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Load and explore bronze data**

# CELL ********************

df_measurements = spark.table(BRONZE_TABLE_MEASUREMENTS)
df_locations = spark.table(BRONZE_TABLE_LOCATIONS)

print(f"Measurements records: {df_measurements.count()}")
print(f"Locations records: {df_locations.count()}")

print("\nMeasurements schema:")
df_measurements.printSchema()

print("\nSample data:")
df_measurements.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Transform measurements to silver**

# CELL ********************

# Transform measurements - now using 'date' column from daily aggregates
df_silver_measurements = df_measurements.withColumn(
    "measurement_date",
    to_date(col("date"))
).withColumn(
    "measurement_year", year(col("measurement_date"))
).withColumn(
    "measurement_month", month(col("measurement_date"))
).withColumn(
    "measurement_day", dayofmonth(col("measurement_date"))
).withColumn(
    "measurement_dayofweek", dayofweek(col("measurement_date"))
)

# Filter valid measurements
df_silver_measurements = df_silver_measurements.filter(
    col("value").isNotNull() & (col("value") >= 0)
)

# Deduplicate
df_silver_measurements = df_silver_measurements.dropDuplicates()

print(f"Silver measurements: {df_silver_measurements.count()}")

print("\nRecords by year:")
df_silver_measurements.groupBy("measurement_year").count().orderBy("measurement_year").show()

# Save tables
df_silver_measurements.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(SILVER_TABLE_MEASUREMENTS)
print(f"Saved: {SILVER_TABLE_MEASUREMENTS}")

df_locations.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(SILVER_TABLE_LOCATIONS)
print(f"Saved: {SILVER_TABLE_LOCATIONS}")

print("\nOpenAQ Silver transformation complete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
