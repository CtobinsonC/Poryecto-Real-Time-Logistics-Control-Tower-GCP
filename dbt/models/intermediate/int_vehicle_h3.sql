-- models/intermediate/int_vehicle_h3.sql
-- Enriquecimiento geoespacial: asigna cada evento a una zona hexagonal
-- Usa ST_GEOHASH como proxy de zonificación (BigQuery no soporta H3 nativo)
-- Resolución 6 de geohash ≈ zona de ~1.2km — equivalente a H3 resolución 8

WITH stops AS (
    SELECT * FROM {{ ref('int_vehicle_stops') }}
)

SELECT
    vehicle_id,
    event_ts,
    latitude,
    longitude,
    geo_point,
    speed_kmh,
    fuel_level_pct,
    is_low_fuel,
    is_stopped,
    stop_duration_min,

    -- Zona geoespacial con ST_GEOHASH (equivalente funcional a H3)
    -- Resolución 6: celdas de ~1.2km x 0.6km (buen balance para análisis de calor)
    ST_GEOHASH(geo_point, 6)  AS zone_hash,

    -- Para análisis de calor: resolución más fina (zona de ~150m)
    ST_GEOHASH(geo_point, 8)  AS zone_hash_fine,

    -- Prefijo de zona para agrupación regional (resolución 4 ≈ barrio)
    ST_GEOHASH(geo_point, 4)  AS zone_region

FROM stops
