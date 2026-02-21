# Flight Tracking Dashboard

Lakeview dashboard -- recreate manually in Databricks UI.

## Datasets

1. **flights_by_country.sql** -- Bar chart, X: origin_country, Y: aircraft_count
2. **altitude_distribution.sql** -- Bar chart, X: altitude_category, Y: unique_aircraft
3. **aircraft_over_time.sql** -- Line chart, X: hour, Y: aircraft_count
4. **aircraft_positions.sql** -- Map, lat/long with color by origin_country

## Layout
```
+---------------------------+---------------------------+
|   Flights by Country      |   Altitude Distribution   |
|   (bar chart)             |   (bar chart)             |
+---------------------------+---------------------------+
|   Aircraft Over Time      |   Aircraft Positions      |
|   (line chart)            |   (map)                   |
+---------------------------+---------------------------+
```
