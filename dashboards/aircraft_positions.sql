SELECT 
  icao24,
  callsign,
  origin_country,
  latitude,
  longitude,
  baro_altitude,
  velocity,
  on_ground
FROM flight_tracking.silver.flight_states
WHERE latitude IS NOT NULL 
  AND longitude IS NOT NULL
  AND _source_timestamp = (
    SELECT MAX(_source_timestamp) 
    FROM flight_tracking.silver.flight_states
  )