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

import requests
import time
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

API_KEY = "107bc627748d059dc170887c882b553a9dd41802011ad83364a49e2da5223a31"
BASE_URL = "https://api.openaq.org/v3"
HEADERS = {
    "X-API-Key": API_KEY,
    "Content-Type": "application/json"
}

US_COUNTRY_ID = 155
NYC_BOUNDING_BOX = {
    "min_lat": 40.4,
    "max_lat": 41.0,
    "min_lon": -74.3,
    "max_lon": -73.7
}

DATE_FROM = "2015-01-01"
DATE_TO = "2024-10-31"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **API Functions**

# CELL ********************

def fetch_locations_by_country(country_id, limit=1000):
    url = f"{BASE_URL}/locations"
    params = {
        "countries_id": country_id,
        "limit": limit
    }
    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None

def filter_nyc_locations(locations):
    nyc_locations = []
    for loc in locations:
        coords = loc.get('coordinates', {})
        lat = coords.get('latitude')
        lon = coords.get('longitude')
        if lat and lon:
            if (NYC_BOUNDING_BOX['min_lat'] <= lat <= NYC_BOUNDING_BOX['max_lat'] and
                NYC_BOUNDING_BOX['min_lon'] <= lon <= NYC_BOUNDING_BOX['max_lon']):
                nyc_locations.append(loc)
    return nyc_locations

def fetch_sensor_daily_measurements(sensor_id, date_from, date_to, limit=1000):
    url = f"{BASE_URL}/sensors/{sensor_id}/days"
    params = {
        "date_from": date_from,
        "date_to": date_to,
        "limit": limit
    }
    try:
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code == 200:
            return response.json().get('results', [])
        else:
            return []
    except Exception as e:
        print(f"Error fetching sensor {sensor_id}: {e}")
        return []

def collect_daily_measurements(locations, date_from, date_to):
    all_measurements = []
    
    for loc in locations:
        location_id = loc.get('id')
        location_name = loc.get('name', 'Unknown')
        coords = loc.get('coordinates', {})
        lat = coords.get('latitude')
        lon = coords.get('longitude')
        timezone = loc.get('timezone', 'UTC')
        
        sensors = loc.get('sensors', [])
        for sensor in sensors:
            sensor_id = sensor.get('id')
            param = sensor.get('parameter', {})
            param_id = param.get('id')
            param_name = param.get('name')
            param_units = param.get('units')
            
            measurements = fetch_sensor_daily_measurements(sensor_id, date_from, date_to)
            
            for m in measurements:
                period = m.get('period', {})
                dt_from = period.get('datetimeFrom', {}).get('utc')
                
                all_measurements.append({
                    'date': dt_from[:10] if dt_from else None,
                    'datetime_utc': dt_from,
                    'location_id': location_id,
                    'location_name': location_name,
                    'latitude': lat,
                    'longitude': lon,
                    'sensor_id': sensor_id,
                    'parameter_id': param_id,
                    'parameter_name': param_name,
                    'parameter_units': param_units,
                    'value': m.get('value'),
                    'timezone': timezone
                })
            
            time.sleep(0.2)
        
        print(f"Processed: {location_name}")
    
    return all_measurements

def prepare_locations_data(locations):
    locations_data = []
    for loc in locations:
        coords = loc.get('coordinates', {})
        entity = loc.get('entity')
        locations_data.append({
            'location_id': loc.get('id'),
            'location_name': loc.get('name', 'Unknown'),
            'latitude': float(coords.get('latitude')) if coords.get('latitude') else 0.0,
            'longitude': float(coords.get('longitude')) if coords.get('longitude') else 0.0,
            'timezone': loc.get('timezone', 'UTC'),
            'is_mobile': bool(loc.get('isMobile', False)),
            'entity_type': entity.get('type') if entity and entity.get('type') else 'Unknown'
        })
    return locations_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Fetch and Save Data**

# CELL ********************

spark = SparkSession.builder.getOrCreate()

print("Fetching US locations...")
us_data = fetch_locations_by_country(US_COUNTRY_ID)
us_locations = us_data.get('results', [])

nyc_locations = filter_nyc_locations(us_locations)
print(f"Found {len(nyc_locations)} NYC area locations")

print(f"Collecting daily measurements from {DATE_FROM} to {DATE_TO}...")
measurements = collect_daily_measurements(nyc_locations, DATE_FROM, DATE_TO)
print(f"Total measurements: {len(measurements)}")

if measurements:
    df_measurements = spark.createDataFrame(measurements)
    df_measurements.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("bronze_openaq_measurements")
    print(f"Measurements saved to table: bronze_openaq_measurements")
else:
    print("No measurements collected")

locations_data = prepare_locations_data(nyc_locations)
df_locations = spark.createDataFrame(locations_data)
df_locations.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("bronze_openaq_locations")
print(f"Locations saved to table: bronze_openaq_locations")

print("OpenAQ Bronze ingestion complete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
