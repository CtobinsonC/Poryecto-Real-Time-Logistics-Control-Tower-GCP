-- models/intermediate/int_vehicle_stops.sql
-- Detecta paradas usando funciones de ventana LAG
-- Materialización: TABLE (lógica pesada, mejor materializar)

WITH staged AS (
    SELECT * FROM {{ ref('stg_telemetry_events') }}
),

with_previous AS (
    SELECT
        vehicle_id,
        event_ts,
        latitude,
        longitude,
        geo_point,
        speed_kmh,
        fuel_level_pct,
        is_low_fuel,
        is_stationary,

        -- Posición y timestamp del evento ANTERIOR por vehículo
        LAG(latitude)  OVER (PARTITION BY vehicle_id ORDER BY event_ts) AS prev_latitude,
        LAG(longitude) OVER (PARTITION BY vehicle_id ORDER BY event_ts) AS prev_longitude,
        LAG(geo_point) OVER (PARTITION BY vehicle_id ORDER BY event_ts) AS prev_geo_point,
        LAG(event_ts)  OVER (PARTITION BY vehicle_id ORDER BY event_ts) AS prev_event_ts

    FROM staged
),

stop_detection AS (
    SELECT
        *,

        -- Distancia respecto al evento anterior (en metros)
        CASE
            WHEN prev_geo_point IS NOT NULL
            THEN ST_DISTANCE(
                geo_point,
                prev_geo_point
            )
            ELSE NULL
        END AS distance_from_prev_m,

        -- Tiempo transcurrido desde el evento anterior (en minutos)
        CASE
            WHEN prev_event_ts IS NOT NULL
            THEN TIMESTAMP_DIFF(event_ts, prev_event_ts, MINUTE)
            ELSE NULL
        END AS minutes_since_prev

    FROM with_previous
),

classified AS (
    SELECT
        vehicle_id,
        event_ts,
        latitude,
        longitude,
        geo_point,
        speed_kmh,
        fuel_level_pct,
        is_low_fuel,
        distance_from_prev_m,
        minutes_since_prev,

        -- Clasificación de parada:
        -- Criterio 1: no se movió > 50m en > 10 minutos
        -- Criterio 2: velocidad < 5 km/h (is_stationary)
        CASE
            WHEN is_stationary THEN TRUE
            WHEN distance_from_prev_m < 50
             AND minutes_since_prev    > 10 THEN TRUE
            ELSE FALSE
        END AS is_stopped,

        -- Duración acumulada de parada (en minutos)
        -- Solo para el primer criterio (diferencia temporal)
        CASE
            WHEN is_stationary                         THEN minutes_since_prev
            WHEN distance_from_prev_m < 50
             AND minutes_since_prev    > 10            THEN minutes_since_prev
            ELSE NULL
        END AS stop_duration_min

    FROM stop_detection
)

SELECT * FROM classified
