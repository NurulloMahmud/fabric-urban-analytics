CREATE TABLE [dbo].[FactTaxiDaily] (

	[year] int NULL, 
	[month] int NULL, 
	[day] int NULL, 
	[date_key] int NULL, 
	[pickup_zone_id] bigint NULL, 
	[dropoff_zone_id] bigint NULL, 
	[taxi_type] varchar(8000) NULL, 
	[trip_count] int NULL, 
	[total_passengers] bigint NULL, 
	[total_distance] float NULL, 
	[total_fare] float NULL, 
	[total_tips] float NULL, 
	[total_revenue] float NULL, 
	[avg_trip_duration] float NULL
);