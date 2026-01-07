CREATE TABLE [dbo].[FactAirQualityDaily] (

	[year] int NULL, 
	[month] int NULL, 
	[day] int NULL, 
	[date_key] int NULL, 
	[location_id] bigint NULL, 
	[location_name] varchar(8000) NULL, 
	[parameter_name] varchar(8000) NULL, 
	[parameter_units] varchar(8000) NULL, 
	[measurement_count] int NULL, 
	[avg_value] float NULL, 
	[min_value] float NULL, 
	[max_value] float NULL
);