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
    when, round as spark_round, unix_timestamp, lit, to_timestamp
)

spark = SparkSession.builder.getOrCreate()

# Drop temp table if exists from previous run
spark.sql("DROP TABLE IF EXISTS temp_taxi_combined")

BRONZE_PATH_YELLOW = "Files/bronze/taxi_yellow/"
BRONZE_PATH_GREEN = "Files/bronze/taxi_green/"
SILVER_TABLE = "silver_taxi_trips"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Helper functions**

# CELL ********************

def standardize_yellow_df(df):
    """Standardize yellow taxi dataframe columns to consistent types"""
    cols = df.columns
    
    # Cast timestamps to STRING first to avoid type conflicts, then we'll convert later
    return df.selectExpr(
        "CAST(VendorID AS LONG) AS VendorID",
        "CAST(tpep_pickup_datetime AS STRING) AS pickup_datetime_str",
        "CAST(tpep_dropoff_datetime AS STRING) AS dropoff_datetime_str",
        "CAST(passenger_count AS DOUBLE) AS passenger_count",
        "CAST(trip_distance AS DOUBLE) AS trip_distance",
        "CAST(RatecodeID AS DOUBLE) AS RatecodeID",
        "CAST(store_and_fwd_flag AS STRING) AS store_and_fwd_flag",
        "CAST(PULocationID AS LONG) AS PULocationID",
        "CAST(DOLocationID AS LONG) AS DOLocationID",
        "CAST(payment_type AS LONG) AS payment_type",
        "CAST(fare_amount AS DOUBLE) AS fare_amount",
        "CAST(extra AS DOUBLE) AS extra",
        "CAST(mta_tax AS DOUBLE) AS mta_tax",
        "CAST(tip_amount AS DOUBLE) AS tip_amount",
        "CAST(tolls_amount AS DOUBLE) AS tolls_amount",
        "CAST(improvement_surcharge AS DOUBLE) AS improvement_surcharge",
        "CAST(total_amount AS DOUBLE) AS total_amount",
        f"CAST({'congestion_surcharge' if 'congestion_surcharge' in cols else 'NULL'} AS DOUBLE) AS congestion_surcharge",
        f"CAST({'Airport_fee' if 'Airport_fee' in cols else 'NULL'} AS DOUBLE) AS Airport_fee",
        "CAST(NULL AS DOUBLE) AS ehail_fee",
        "CAST(NULL AS LONG) AS trip_type",
        "'yellow' AS taxi_type"
    )

def standardize_green_df(df):
    """Standardize green taxi dataframe columns to consistent types"""
    cols = df.columns
    
    return df.selectExpr(
        "CAST(VendorID AS LONG) AS VendorID",
        "CAST(lpep_pickup_datetime AS STRING) AS pickup_datetime_str",
        "CAST(lpep_dropoff_datetime AS STRING) AS dropoff_datetime_str",
        "CAST(passenger_count AS DOUBLE) AS passenger_count",
        "CAST(trip_distance AS DOUBLE) AS trip_distance",
        "CAST(RatecodeID AS DOUBLE) AS RatecodeID",
        "CAST(store_and_fwd_flag AS STRING) AS store_and_fwd_flag",
        "CAST(PULocationID AS LONG) AS PULocationID",
        "CAST(DOLocationID AS LONG) AS DOLocationID",
        "CAST(payment_type AS LONG) AS payment_type",
        "CAST(fare_amount AS DOUBLE) AS fare_amount",
        "CAST(extra AS DOUBLE) AS extra",
        "CAST(mta_tax AS DOUBLE) AS mta_tax",
        "CAST(tip_amount AS DOUBLE) AS tip_amount",
        "CAST(tolls_amount AS DOUBLE) AS tolls_amount",
        "CAST(improvement_surcharge AS DOUBLE) AS improvement_surcharge",
        "CAST(total_amount AS DOUBLE) AS total_amount",
        f"CAST({'congestion_surcharge' if 'congestion_surcharge' in cols else 'NULL'} AS DOUBLE) AS congestion_surcharge",
        "CAST(NULL AS DOUBLE) AS Airport_fee",
        f"CAST({'ehail_fee' if 'ehail_fee' in cols else 'NULL'} AS DOUBLE) AS ehail_fee",
        f"CAST({'trip_type' if 'trip_type' in cols else 'NULL'} AS LONG) AS trip_type",
        "'green' AS taxi_type"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Process Yellow Files One by One and Save to Delta**

# CELL ********************

yellow_files = [f.path for f in mssparkutils.fs.ls(BRONZE_PATH_YELLOW)]
print(f"Found {len(yellow_files)} yellow taxi files")

first_file = True
error_count = 0
for i, file_path in enumerate(yellow_files):
    try:
        df = spark.read.parquet(file_path)
        df_std = standardize_yellow_df(df)
        
        if first_file:
            df_std.write.mode("overwrite").format("delta").saveAsTable("temp_taxi_combined")
            first_file = False
        else:
            df_std.write.mode("append").format("delta").saveAsTable("temp_taxi_combined")
        
        if (i + 1) % 10 == 0:
            print(f"Processed {i + 1}/{len(yellow_files)} yellow files")
    except Exception as e:
        error_count += 1
        print(f"Error processing {file_path.split('/')[-1]}: {str(e)[:100]}")

print(f"Completed processing yellow files. Errors: {error_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Process Green Files**

# CELL ********************

green_files = [f.path for f in mssparkutils.fs.ls(BRONZE_PATH_GREEN)]
print(f"Found {len(green_files)} green taxi files")

error_count = 0
for i, file_path in enumerate(green_files):
    try:
        df = spark.read.parquet(file_path)
        df_std = standardize_green_df(df)
        df_std.write.mode("append").format("delta").saveAsTable("temp_taxi_combined")
        
        if (i + 1) % 10 == 0:
            print(f"Processed {i + 1}/{len(green_files)} green files")
    except Exception as e:
        error_count += 1
        print(f"Error processing {file_path.split('/')[-1]}: {str(e)[:100]}")

print(f"Completed processing green files. Errors: {error_count}")

df_combined = spark.table("temp_taxi_combined")
print(f"Total combined records: {df_combined.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# _**Transform to Silver**_

# CELL ********************

df_bronze = spark.table("temp_taxi_combined")

# Convert string timestamps back to timestamp type
df_bronze = df_bronze.withColumn("pickup_datetime", to_timestamp(col("pickup_datetime_str"))) \
    .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime_str"))) \
    .drop("pickup_datetime_str", "dropoff_datetime_str")

df_silver = df_bronze.filter(
    (col("pickup_datetime") >= "2015-01-01") &
    (col("pickup_datetime") < "2025-01-01") &
    (col("fare_amount") >= 0) &
    (col("total_amount") >= 0) &
    (col("trip_distance") >= 0) &
    (col("trip_distance") <= 500)
)

df_silver = df_silver.withColumn(
    "passenger_count",
    when(col("passenger_count").isNull(), 1.0).otherwise(col("passenger_count"))
).withColumn(
    "RatecodeID",
    when(col("RatecodeID").isNull(), 1.0).otherwise(col("RatecodeID"))
).withColumn(
    "store_and_fwd_flag",
    when(col("store_and_fwd_flag").isNull(), "N").otherwise(col("store_and_fwd_flag"))
).withColumn(
    "congestion_surcharge",
    when(col("congestion_surcharge").isNull(), 0.0).otherwise(col("congestion_surcharge"))
).withColumn(
    "Airport_fee",
    when(col("Airport_fee").isNull(), 0.0).otherwise(col("Airport_fee"))
)

df_silver = df_silver.withColumn("pickup_year", year(col("pickup_datetime"))) \
    .withColumn("pickup_month", month(col("pickup_datetime"))) \
    .withColumn("pickup_day", dayofmonth(col("pickup_datetime"))) \
    .withColumn("pickup_hour", hour(col("pickup_datetime"))) \
    .withColumn("pickup_dayofweek", dayofweek(col("pickup_datetime")))

df_silver = df_silver.withColumn(
    "trip_duration_minutes",
    spark_round(
        (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60,
        2
    )
)

df_silver = df_silver.filter(
    (col("trip_duration_minutes") > 0) &
    (col("trip_duration_minutes") <= 180)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Deduplicate and Save**

# CELL ********************

df_silver.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(SILVER_TABLE)

print(f"Silver table saved: {SILVER_TABLE}")

silver_count = spark.table(SILVER_TABLE).count()
print(f"Silver records: {silver_count}")

print("\nRecords by taxi type:")
spark.table(SILVER_TABLE).groupBy("taxi_type").count().show()

print("\nRecords by year:")
spark.table(SILVER_TABLE).groupBy("pickup_year").count().orderBy("pickup_year").show(15)

spark.sql("DROP TABLE IF EXISTS temp_taxi_combined")
print("Temp table cleaned up")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
