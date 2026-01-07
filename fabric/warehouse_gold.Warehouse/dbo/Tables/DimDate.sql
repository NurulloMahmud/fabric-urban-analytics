CREATE TABLE [dbo].[DimDate] (

	[date_key] int NULL, 
	[date] date NULL, 
	[year] int NULL, 
	[quarter] int NULL, 
	[month] int NULL, 
	[day_of_month] int NULL, 
	[day_of_week] int NULL, 
	[is_weekend] int NOT NULL, 
	[week_of_year] int NULL
);