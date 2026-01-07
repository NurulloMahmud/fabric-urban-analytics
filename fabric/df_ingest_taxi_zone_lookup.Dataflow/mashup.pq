[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Query_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Query = let
  Source = Csv.Document(Web.Contents("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"), [Delimiter = ",", Columns = 4, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"LocationID", Int64.Type}, {"Borough", type text}, {"Zone", type text}, {"service_zone", type text}})
in
  #"Changed column type";
shared Query_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "6484574c-17ac-4a0b-8124-3be53dd6f724"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1575b5e8-95e6-4f43-84e6-99ca5ca51d40"]}[Data],
  TableNavigation = Navigation_2{[Id = "bronze_taxi_zone_lookup", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
