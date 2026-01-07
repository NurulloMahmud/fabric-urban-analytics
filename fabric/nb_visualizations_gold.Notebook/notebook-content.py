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

%pip install matplotlib seaborn plotly --quiet

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("husl")

pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x)

print("Libraries imported")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Load Data from Warehouse**

# CELL ********************

df_taxi = spark.sql("""
    SELECT 
        year,
        month,
        taxi_type,
        pickup_zone_id,
        dropoff_zone_id,
        SUM(trip_count) AS trip_count,
        SUM(total_passengers) AS total_passengers,
        SUM(total_distance) AS total_distance,
        SUM(total_fare) AS total_fare,
        SUM(total_tips) AS total_tips,
        SUM(total_revenue) AS total_revenue,
        AVG(avg_trip_duration) AS avg_trip_duration
    FROM warehouse_gold.dbo.FactTaxiDaily
    GROUP BY year, month, taxi_type, pickup_zone_id, dropoff_zone_id
""").toPandas()

df_air = spark.sql("""
    SELECT 
        year,
        month,
        location_id,
        location_name,
        parameter_name,
        parameter_units,
        SUM(measurement_count) AS measurement_count,
        AVG(avg_value) AS avg_value,
        MIN(min_value) AS min_value,
        MAX(max_value) AS max_value
    FROM warehouse_gold.dbo.FactAirQualityDaily
    GROUP BY year, month, location_id, location_name, parameter_name, parameter_units
""").toPandas()

df_date = spark.sql("""
    SELECT * FROM warehouse_gold.dbo.DimDate
""").toPandas()

df_zone = spark.sql("""
    SELECT * FROM warehouse_gold.dbo.DimZone
""").toPandas()

df_fx = spark.sql("""
    SELECT * FROM warehouse_gold.dbo.DimFX
""").toPandas()

df_gdp = spark.sql("""
    SELECT * FROM warehouse_gold.dbo.DimGDP
""").toPandas()


print(f"Data loaded successfully...")
print(f"• FactTaxiDaily: {len(df_taxi):,} rows")
print(f"• FactAirQualityDaily: {len(df_air):,} rows")
print(f"• DimDate: {len(df_date):,} rows")
print(f"• DimZone: {len(df_zone):,} rows")
print(f"• DimFX: {len(df_fx):,} rows")
print(f"• DimGDP: {len(df_gdp):,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Mobility Dashboard**

# CELL ********************

print("=" * 60)
print("DASHBOARD 1: MOBILITY DASHBOARD")
print("=" * 60)

fig, axes = plt.subplots(2, 2, figsize=(16, 12))

# monthly trip trends
monthly_trips = df_taxi.groupby(['year', 'month']).agg({
    'trip_count': 'sum'
}).reset_index()
monthly_trips['year_month'] = monthly_trips['year'].astype(str) + '-' + monthly_trips['month'].astype(str).str.zfill(2)
monthly_trips = monthly_trips.sort_values(['year', 'month'])

ax1 = axes[0, 0]
ax1.plot(range(len(monthly_trips)), monthly_trips['trip_count'] / 1e6, marker='o', linewidth=2, markersize=3)
ax1.set_title('Monthly Trip Volume Over Time', fontsize=14, fontweight='bold')
ax1.set_xlabel('Time Period')
ax1.set_ylabel('Total Trips (Millions)')
year_positions = monthly_trips.groupby('year').first().reset_index()
ax1.set_xticks([monthly_trips[monthly_trips['year'] == y].index[0] for y in monthly_trips['year'].unique()[::2]])
ax1.set_xticklabels(monthly_trips['year'].unique()[::2], rotation=45)

# average fares over time
monthly_fare = df_taxi.groupby(['year', 'month']).agg({
    'total_fare': 'sum',
    'trip_count': 'sum'
}).reset_index()
monthly_fare['avg_fare'] = monthly_fare['total_fare'] / monthly_fare['trip_count']
monthly_fare = monthly_fare.sort_values(['year', 'month'])

ax2 = axes[0, 1]
ax2.plot(range(len(monthly_fare)), monthly_fare['avg_fare'], marker='s', linewidth=2, markersize=3, color='green')
ax2.set_title('Average Fare Per Trip Over Time', fontsize=14, fontweight='bold')
ax2.set_xlabel('Time Period')
ax2.set_ylabel('Average Fare ($)')
ax2.set_xticks([i for i in range(0, len(monthly_fare), 24)])
ax2.set_xticklabels(monthly_fare['year'].unique()[::2], rotation=45)

# top 10 busiest puckup zones
pickup_zones = df_taxi.groupby('pickup_zone_id').agg({
    'trip_count': 'sum'
}).reset_index()
pickup_zones = pickup_zones.merge(df_zone, left_on='pickup_zone_id', right_on='zone_id', how='left')
top_10_zones = pickup_zones.nlargest(10, 'trip_count')

ax3 = axes[1, 0]
bars = ax3.barh(top_10_zones['zone_name'].fillna('Unknown'), top_10_zones['trip_count'] / 1e6, color='coral')
ax3.set_title('Top 10 Busiest Pickup Zones', fontsize=14, fontweight='bold')
ax3.set_xlabel('Total Trips (Millions)')
ax3.invert_yaxis()

# yellow vs green taxi comparison
taxi_type_compare = df_taxi.groupby('taxi_type').agg({
    'trip_count': 'sum',
    'total_revenue': 'sum'
}).reset_index()

ax4 = axes[1, 1]
x = range(len(taxi_type_compare))
width = 0.35
bars1 = ax4.bar([i - width/2 for i in x], taxi_type_compare['trip_count'] / 1e6, width, label='Trips (Millions)', color='gold')
ax4_twin = ax4.twinx()
bars2 = ax4_twin.bar([i + width/2 for i in x], taxi_type_compare['total_revenue'] / 1e9, width, label='Revenue ($B)', color='darkgreen')
ax4.set_title('Yellow vs Green Taxi Comparison', fontsize=14, fontweight='bold')
ax4.set_xticks(x)
ax4.set_xticklabels(taxi_type_compare['taxi_type'])
ax4.set_ylabel('Trips (Millions)')
ax4_twin.set_ylabel('Revenue ($ Billions)')
ax4.legend(loc='upper left')
ax4_twin.legend(loc='upper right')

plt.tight_layout()
plt.show()

print("\nMOBILITY SUMMARY STATISTICS:")
print(f"• Total Trips: {df_taxi['trip_count'].sum():,.0f}")
print(f"• Total Revenue: ${df_taxi['total_revenue'].sum():,.2f}")
print(f"• Average Fare: ${df_taxi['total_fare'].sum() / df_taxi['trip_count'].sum():.2f}")
print(f"• Total Passengers: {df_taxi['total_passengers'].sum():,.0f}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Air Quality Dashboard**

# CELL ********************

print("=" * 60)
print("DASHBOARD 2: AIR QUALITY DASHBOARD")
print("=" * 60)

print("Available air quality parameters:", df_air['parameter_name'].unique())

# PM2.5 data - main pollutant of interest
df_pm25 = df_air[df_air['parameter_name'].str.lower().str.contains('pm25|pm2.5|pm 2.5', na=False)]

if len(df_pm25) == 0:
    main_param = df_air['parameter_name'].value_counts().index[0]
    df_pm25 = df_air[df_air['parameter_name'] == main_param]
    param_label = main_param
    print(f"Note: PM2.5 data not found, using '{main_param}' instead")
else:
    param_label = "PM2.5"

fig, axes = plt.subplots(2, 2, figsize=(16, 12))

# air quality trends over time
monthly_air = df_pm25.groupby(['year', 'month']).agg({
    'avg_value': 'mean'
}).reset_index()
monthly_air = monthly_air.sort_values(['year', 'month'])

ax1 = axes[0, 0]
ax1.plot(range(len(monthly_air)), monthly_air['avg_value'], marker='o', linewidth=2, markersize=3, color='purple')
ax1.set_title(f'{param_label} Levels Over Time', fontsize=14, fontweight='bold')
ax1.set_xlabel('Time Period')
ax1.set_ylabel(f'Average {param_label} Value')
ax1.set_xticks([i for i in range(0, len(monthly_air), 12)])
ax1.set_xticklabels([int(y) for y in monthly_air['year'].unique()], rotation=45)

# air quality by location
location_air = df_pm25.groupby('location_name').agg({
    'avg_value': 'mean'
}).reset_index().nlargest(10, 'avg_value')

ax2 = axes[0, 1]
ax2.barh(location_air['location_name'], location_air['avg_value'], color='indianred')
ax2.set_title(f'Top 10 Locations by {param_label} Levels', fontsize=14, fontweight='bold')
ax2.set_xlabel(f'Average {param_label}')
ax2.invert_yaxis()

# seasonal patterns (monthly avg)
seasonal = df_pm25.groupby('month').agg({
    'avg_value': 'mean'
}).reset_index()

ax3 = axes[1, 0]
month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
ax3.bar(seasonal['month'], seasonal['avg_value'], color='teal')
ax3.set_title(f'Seasonal {param_label} Pattern (Monthly Average)', fontsize=14, fontweight='bold')
ax3.set_xlabel('Month')
ax3.set_ylabel(f'Average {param_label}')
ax3.set_xticks(range(1, 13))
ax3.set_xticklabels(month_names)

# year over year comparison
yearly_air = df_pm25.groupby('year').agg({
    'avg_value': 'mean',
    'max_value': 'max'
}).reset_index()

ax4 = axes[1, 1]
x = range(len(yearly_air))
width = 0.35
ax4.bar([i - width/2 for i in x], yearly_air['avg_value'], width, label='Average', color='steelblue')
ax4.bar([i + width/2 for i in x], yearly_air['max_value'], width, label='Max', color='tomato')
ax4.set_title(f'Year-over-Year {param_label} Comparison', fontsize=14, fontweight='bold')
ax4.set_xticks(x)
ax4.set_xticklabels(yearly_air['year'].astype(int))
ax4.set_xlabel('Year')
ax4.set_ylabel(f'{param_label} Value')
ax4.legend()

plt.tight_layout()
plt.show()

print(f"\nAIR QUALITY SUMMARY ({param_label}):")
print(f"• Average Level: {df_pm25['avg_value'].mean():.2f}")
print(f"• Max Recorded: {df_pm25['max_value'].max():.2f}")
print(f"• Min Recorded: {df_pm25['min_value'].min():.2f}")
print(f"• Number of Locations: {df_pm25['location_name'].nunique()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Correlation Dashboard**

# CELL ********************

print("=" * 60)
print("DASHBOARD 3: MOBILITY VS AIR QUALITY CORRELATION")
print("=" * 60)

# aggregate taxi data by year - month
taxi_monthly = df_taxi.groupby(['year', 'month']).agg({
    'trip_count': 'sum',
    'total_revenue': 'sum'
}).reset_index()

# aggregate air quality data by year-month
air_monthly = df_pm25.groupby(['year', 'month']).agg({
    'avg_value': 'mean'
}).reset_index()


correlation_df = taxi_monthly.merge(air_monthly, on=['year', 'month'], how='inner')
correlation_df['year_month'] = correlation_df['year'].astype(str) + '-' + correlation_df['month'].astype(str).str.zfill(2)

fig, axes = plt.subplots(2, 2, figsize=(16, 12))

# trips vs air quality
ax1 = axes[0, 0]
ax1.scatter(correlation_df['trip_count'] / 1e6, correlation_df['avg_value'], alpha=0.7, c='blue', s=50)
ax1.set_title(f'Monthly Trips vs {param_label} Levels', fontsize=14, fontweight='bold')
ax1.set_xlabel('Monthly Trip Count (Millions)')
ax1.set_ylabel(f'{param_label} Level')

# ad correlation coefficient
corr = correlation_df['trip_count'].corr(correlation_df['avg_value'])
ax1.annotate(f'Correlation: {corr:.3f}', xy=(0.05, 0.95), xycoords='axes fraction', fontsize=12,
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

# chart-2 -> dual-axis time serier
correlation_df_sorted = correlation_df.sort_values(['year', 'month'])

ax2 = axes[0, 1]
ax2.plot(range(len(correlation_df_sorted)), correlation_df_sorted['trip_count'] / 1e6, 'b-', linewidth=2, label='Trips (M)')
ax2.set_ylabel('Trips (Millions)', color='blue')
ax2.tick_params(axis='y', labelcolor='blue')

ax2_twin = ax2.twinx()
ax2_twin.plot(range(len(correlation_df_sorted)), correlation_df_sorted['avg_value'], 'r-', linewidth=2, label=param_label)
ax2_twin.set_ylabel(f'{param_label} Level', color='red')
ax2_twin.tick_params(axis='y', labelcolor='red')

ax2.set_title(f'Monthly Trips vs {param_label} Overlay', fontsize=14, fontweight='bold')
ax2.set_xlabel('Time Period')
ax2.legend(loc='upper left')
ax2_twin.legend(loc='upper right')

# chart3 - trips by month (seasonal patterns)
monthly_pattern = df_taxi.groupby('month').agg({
    'trip_count': 'sum'
}).reset_index()

ax3 = axes[1, 0]
month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
ax3.bar(monthly_pattern['month'], monthly_pattern['trip_count'] / 1e6, color='steelblue')
ax3.set_title('Seasonal Trip Pattern (Monthly Total)', fontsize=14, fontweight='bold')
ax3.set_xlabel('Month')
ax3.set_ylabel('Total Trips (Millions)')
ax3.set_xticks(range(1, 13))
ax3.set_xticklabels(month_names)

# chart4 - correlation by year
yearly_corr = []
for year in correlation_df['year'].unique():
    year_data = correlation_df[correlation_df['year'] == year]
    if len(year_data) > 2:
        c = year_data['trip_count'].corr(year_data['avg_value'])
        yearly_corr.append({'year': year, 'correlation': c})

if yearly_corr:
    yearly_corr_df = pd.DataFrame(yearly_corr)
    ax4 = axes[1, 1]
    colors = ['green' if c > 0 else 'red' for c in yearly_corr_df['correlation']]
    ax4.bar(yearly_corr_df['year'].astype(int), yearly_corr_df['correlation'], color=colors)
    ax4.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    ax4.set_title(f'Yearly Correlation: Trips vs {param_label}', fontsize=14, fontweight='bold')
    ax4.set_xlabel('Year')
    ax4.set_ylabel('Correlation Coefficient')
else:
    axes[1, 1].text(0.5, 0.5, 'Insufficient data for yearly correlation', ha='center', va='center')
    axes[1, 1].set_title('Yearly Correlation', fontsize=14, fontweight='bold')

plt.tight_layout()
plt.show()

print(f"\nCORRELATION INSIGHTS:")
print(f"• Overall Correlation (Trips vs {param_label}): {corr:.3f}")
if corr > 0.3:
    print(f" -> Positive correlation: Higher traffic may be associated with worse air quality")
elif corr < -0.3:
    print(f" -> Negative correlation: Higher traffic may be associated with better air quality")
else:
    print(f" -> Weak correlation: No strong linear relationship detected")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Economic dashboard**

# CELL ********************

print("=" * 60)
print("DASHBOARD 4: ECONOMIC IMPACT DASHBOARD")
print("=" * 60)

fx_monthly = df_fx.groupby(['fx_year', 'fx_month']).agg({
    'usd_eur_rate': 'mean'
}).reset_index()
fx_monthly.columns = ['year', 'month', 'usd_eur_rate']

taxi_monthly_rev = df_taxi.groupby(['year', 'month']).agg({
    'total_revenue': 'sum',
    'trip_count': 'sum'
}).reset_index()

taxi_with_fx = taxi_monthly_rev.merge(fx_monthly, on=['year', 'month'], how='left')

# calculate revenue in EUR
taxi_with_fx['usd_eur_rate'] = taxi_with_fx['usd_eur_rate'].fillna(taxi_with_fx['usd_eur_rate'].mean())
taxi_with_fx['total_revenue_eur'] = taxi_with_fx['total_revenue'] / taxi_with_fx['usd_eur_rate']
taxi_with_fx = taxi_with_fx.sort_values(['year', 'month'])

us_gdp = df_gdp[df_gdp['country_code'] == 'USA'].copy()

fig, axes = plt.subplots(2, 2, figsize=(16, 12))

# revenue USD vs EUR over time
ax1 = axes[0, 0]
ax1.plot(range(len(taxi_with_fx)), taxi_with_fx['total_revenue'] / 1e6, 'b-', linewidth=2, label='USD')
ax1.plot(range(len(taxi_with_fx)), taxi_with_fx['total_revenue_eur'] / 1e6, 'g-', linewidth=2, label='EUR')
ax1.set_title('Monthly Revenue: USD vs EUR', fontsize=14, fontweight='bold')
ax1.set_xlabel('Time Period')
ax1.set_ylabel('Revenue (Millions)')
ax1.legend()
ax1.set_xticks([i for i in range(0, len(taxi_with_fx), 12)])
ax1.set_xticklabels([int(y) for y in taxi_with_fx['year'].unique()], rotation=45)

# chart 2 - excgange rate trends
fx_trend = df_fx.groupby(['fx_year', 'fx_month']).agg({
    'usd_eur_rate': 'mean'
}).reset_index()
fx_trend = fx_trend.sort_values(['fx_year', 'fx_month'])

ax2 = axes[0, 1]
ax2.plot(range(len(fx_trend)), fx_trend['usd_eur_rate'], 'purple', linewidth=2)
ax2.set_title('USD/EUR Exchange Rate Over Time', fontsize=14, fontweight='bold')
ax2.set_xlabel('Time Period')
ax2.set_ylabel('USD/EUR Rate')
ax2.axhline(y=1.0, color='red', linestyle='--', alpha=0.5, label='Parity')
ax2.legend()
ax2.set_xticks([i for i in range(0, len(fx_trend), 12)])
ax2.set_xticklabels([int(y) for y in fx_trend['fx_year'].unique()], rotation=45)

# chart 3 - yearly revenue with GDP context
yearly_revenue = taxi_with_fx.groupby('year').agg({
    'total_revenue': 'sum'
}).reset_index()
yearly_revenue = yearly_revenue.merge(us_gdp[['year', 'gdp_usd']], on='year', how='left')

ax3 = axes[1, 0]
ax3.bar(yearly_revenue['year'].astype(int), yearly_revenue['total_revenue'] / 1e9, color='steelblue', label='Taxi Revenue ($B)')
ax3.set_title('Yearly Taxi Revenue', fontsize=14, fontweight='bold')
ax3.set_xlabel('Year')
ax3.set_ylabel('Revenue ($ Billions)')

# GDP as secondary axis if available
if yearly_revenue['gdp_usd'].notna().any():
    ax3_twin = ax3.twinx()
    ax3_twin.plot(yearly_revenue['year'].astype(int), yearly_revenue['gdp_usd'] / 1e12, 'ro-', linewidth=2, label='US GDP ($T)')
    ax3_twin.set_ylabel('US GDP ($ Trillions)', color='red')
    ax3_twin.tick_params(axis='y', labelcolor='red')
    ax3_twin.legend(loc='upper right')
ax3.legend(loc='upper left')

# chart 4: year over year revenue growth
yearly_revenue['revenue_growth'] = yearly_revenue['total_revenue'].pct_change() * 100

ax4 = axes[1, 1]
colors = ['green' if g > 0 else 'red' for g in yearly_revenue['revenue_growth'].fillna(0)]
ax4.bar(yearly_revenue['year'].astype(int), yearly_revenue['revenue_growth'].fillna(0), color=colors)
ax4.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
ax4.set_title('Year-over-Year Revenue Growth (%)', fontsize=14, fontweight='bold')
ax4.set_xlabel('Year')
ax4.set_ylabel('Growth Rate (%)')

plt.tight_layout()
plt.show()

total_usd = taxi_with_fx['total_revenue'].sum()
total_eur = taxi_with_fx['total_revenue_eur'].sum()
print(f"\nECONOMIC IMPACT SUMMARY:")
print(f"• Total Revenue (USD): ${total_usd:,.2f}")
print(f"• Total Revenue (EUR): €{total_eur:,.2f}")
print(f"• Average USD/EUR Rate: {df_fx['usd_eur_rate'].mean():.4f}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Summary & Key Insights**

# CELL ********************

print("=" * 60)
print("PROJECT SUMMARY: URBAN ANALYTICS PLATFORM")
print("=" * 60)

print("\nDATA OVERVIEW\n ")
print(f"• Taxi Trips Analyzed:     {df_taxi['trip_count'].sum():>20,}  │")
print(f"• Air Quality Records:     {len(df_air):>20,}  │")
print(f"• Date Range:              {df_date['year'].min()} - {df_date['year'].max():>14}  │")
print(f"• NYC Zones Covered:       {df_zone['zone_id'].nunique():>20}  │")
print(f"• FX Data Points:          {len(df_fx):>20,}  │")
print(f"• Countries (GDP):         {df_gdp['country_code'].nunique():>20}  │")
print("-" * 60)

print("""
KEY ANALYTICAL QUESTIONS ANSWERED:
──────────────────────────────────
1. How does traffic intensity relate to air quality in NYC?
2. Which zones/times show strongest taxi demand?
3. What is average revenue in USD vs EUR?
4. How do mobility/economic trends compare over time?

DASHBOARDS CREATED:
───────────────────
1. Mobility Dashboard - Trip volumes, fares, busiest zones
2. Air Quality Dashboard - Pollution trends, seasonal patterns
3. Correlation Dashboard - Mobility vs Air Quality analysis
4. Economic Dashboard - Revenue analysis, GDP context
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
