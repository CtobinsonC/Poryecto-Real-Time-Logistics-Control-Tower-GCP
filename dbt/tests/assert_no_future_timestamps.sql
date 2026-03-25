-- tests/assert_no_future_timestamps.sql
-- Test singular: verifica que ningún evento tiene timestamp futuro
-- Si devuelve filas → el test FALLA

SELECT
    vehicle_id,
    event_ts,
    CURRENT_TIMESTAMP() AS checked_at
FROM {{ ref('stg_telemetry_events') }}
WHERE event_ts > TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
