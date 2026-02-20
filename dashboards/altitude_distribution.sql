SELECT 
  altitude_category,
  COUNT(*) AS readings,
  COUNT(DISTINCT icao24) AS unique_aircraft
FROM flight_tracking.silver.flight_states
GROUP BY altitude_category