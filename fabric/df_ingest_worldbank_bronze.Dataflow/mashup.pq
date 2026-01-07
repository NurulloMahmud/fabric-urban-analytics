[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared GDP_US = let
  Source = Json.Document(Web.Contents("https://api.worldbank.org/v2/country/US/indicator/NY.GDP.MKTP.CD?format=json&date=2015:2024&per_page=100")),
  #"Converted to table" = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  Navigation = #"Converted to table"{1}[Column1],
  #"Converted to table 1" = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to table 1", "Column1", {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}, {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Column1", {"indicator", "country", "unit", "obs_status", "decimal"})
in
  #"Removed columns";
shared GDP_GB = let
  Source = Json.Document(Web.Contents("https://api.worldbank.org/v2/country/GB/indicator/NY.GDP.MKTP.CD?format=json&date=2015:2024&per_page=100")),
  #"Converted to table" = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  Navigation = #"Converted to table"{1}[Column1],
  #"Converted to table 1" = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to table 1", "Column1", {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}, {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Column1", {"indicator", "country", "unit", "obs_status", "decimal"})
in
  #"Removed columns";
shared GDP_DE = let
  Source = Json.Document(Web.Contents("https://api.worldbank.org/v2/country/DE/indicator/NY.GDP.MKTP.CD?format=json&date=2015:2024&per_page=100")),
  #"Converted to table" = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  Navigation = #"Converted to table"{1}[Column1],
  #"Converted to table 1" = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to table 1", "Column1", {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}, {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Column1", {"indicator", "country", "unit", "obs_status", "decimal"})
in
  #"Removed columns";
shared GDP_FR = let
  Source = Json.Document(Web.Contents("https://api.worldbank.org/v2/country/FR/indicator/NY.GDP.MKTP.CD?format=json&date=2015:2024&per_page=100")),
  #"Converted to table" = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  Navigation = #"Converted to table"{1}[Column1],
  #"Converted to table 1" = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to table 1", "Column1", {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}, {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Column1", {"indicator", "country", "unit", "obs_status", "decimal"})
in
  #"Removed columns";
shared GDP_JP = let
  Source = Json.Document(Web.Contents("https://api.worldbank.org/v2/country/JP/indicator/NY.GDP.MKTP.CD?format=json&date=2015:2024&per_page=100")),
  #"Converted to table" = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  Navigation = #"Converted to table"{1}[Column1],
  #"Converted to table 1" = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to table 1", "Column1", {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}, {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Column1", {"indicator", "country", "unit", "obs_status", "decimal"})
in
  #"Removed columns";
shared GDP_CN = let
  Source = Json.Document(Web.Contents("https://api.worldbank.org/v2/country/CN/indicator/NY.GDP.MKTP.CD?format=json&date=2015:2024&per_page=100")),
  #"Converted to table" = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  Navigation = #"Converted to table"{1}[Column1],
  #"Converted to table 1" = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to table 1", "Column1", {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}, {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Column1", {"indicator", "country", "unit", "obs_status", "decimal"})
in
  #"Removed columns";
shared GDP_IN = let
  Source = Json.Document(Web.Contents("https://api.worldbank.org/v2/country/IN/indicator/NY.GDP.MKTP.CD?format=json&date=2015:2024&per_page=100")),
  #"Converted to table" = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  Navigation = #"Converted to table"{1}[Column1],
  #"Converted to table 1" = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to table 1", "Column1", {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}, {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Column1", {"indicator", "country", "unit", "obs_status", "decimal"})
in
  #"Removed columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "GDP_All_Countries_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "countryiso3code", DestinationColumnName = "countryiso3code"], [SourceColumnName = "date", DestinationColumnName = "date"], [SourceColumnName = "value", DestinationColumnName = "value"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared GDP_All_Countries = let
  Source = Table.Combine({GDP_IN, GDP_CN, GDP_DE, GDP_FR, GDP_GB, GDP_JP, GDP_US}),
  #"Changed column type" = Table.TransformColumnTypes(Source, {{"countryiso3code", type text}, {"date", type text}, {"value", type number}})
in
  #"Changed column type";
shared GDP_All_Countries_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "6484574c-17ac-4a0b-8124-3be53dd6f724"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1575b5e8-95e6-4f43-84e6-99ca5ca51d40"]}[Data],
  TableNavigation = Navigation_2{[Id = "GDP_All_Countries", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
