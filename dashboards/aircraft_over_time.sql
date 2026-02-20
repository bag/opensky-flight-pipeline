SELECT 
  DATE_TRUNC('hour', to_timestamp(_source_timestamp)) AS hour,
  COUNT(DISTINCT icao24) AS aircraft_count
FROM flight_tracking.silver.flight_states
GROUP BY 1
ORDER BY 1