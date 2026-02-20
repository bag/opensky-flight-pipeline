SELECT 
  origin_country,
  COUNT(DISTINCT icao24) AS aircraft_count,
  ROUND(AVG(velocity) * 3.6, 1) AS avg_speed_kmh
FROM flight_tracking.silver.flight_states
WHERE origin_country IS NOT NULL
GROUP BY origin_country
ORDER BY aircraft_count DESC
LIMIT 15