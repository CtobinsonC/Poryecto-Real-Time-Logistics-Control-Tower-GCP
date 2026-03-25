-- models/staging/stg_telemetry_events.sql
-- Capa Staging: limpieza y normalización de datos crudos
-- Materialización: VIEW (sin costo de storage, siempre fresco)

WITH source AS (
    SELECT * FROM {{ source('logistics_raw', 'telemetry_events') }}
),

cleaned AS (
    SELECT
        -- Identificadores
        TRIM(vehicle_id)                          AS vehicle_id,

        -- Timestamps
        CAST(event_timestamp AS TIMESTAMP)        AS event_ts,
        CAST(_ingested_at    AS TIMESTAMP)        AS ingested_at,

        -- Coordenadas (filtrar nulos e inválidos)
        CAST(latitude  AS FLOAT64)                AS latitude,
        CAST(longitude AS FLOAT64)                AS longitude,
        geo_point,

        -- Métricas
        ROUND(CAST(speed      AS FLOAT64), 2)     AS speed_kmh,
        ROUND(CAST(fuel_level AS FLOAT64), 1)     AS fuel_level_pct,

        -- Flags de negocio
        CAST(fuel_level AS FLOAT64) < 25          AS is_low_fuel,
        CAST(speed      AS FLOAT64) < 5           AS is_stationary

    FROM source
    WHERE
        -- Filtrar registros inválidos
        vehicle_id   IS NOT NULL
        AND event_timestamp IS NOT NULL
        AND latitude  IS NOT NULL
        AND longitude IS NOT NULL
        AND latitude  BETWEEN -90  AND 90
        AND longitude BETWEEN -180 AND 180
        -- Filtrar timestamps futuros (con 5 min de margen para latencia)
        AND CAST(event_timestamp AS TIMESTAMP) <= TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
)

SELECT * FROM cleaned
