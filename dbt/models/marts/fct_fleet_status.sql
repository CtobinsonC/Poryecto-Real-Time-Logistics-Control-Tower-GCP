-- models/marts/fct_fleet_status.sql
-- Tabla final: última posición conocida y estado actual de cada vehículo
-- Materialización: INCREMENTAL con merge por vehicle_id

{{
    config(
        materialized='incremental',
        unique_key='vehicle_id',
        incremental_strategy='merge',
        partition_by={
            "field": "updated_at",
            "data_type": "timestamp",
            "granularity": "day"
        }
    )
}}

WITH latest_events AS (
    -- Obtener el evento más reciente por vehículo
    SELECT
        vehicle_id,
        event_ts,
        latitude,
        longitude,
        geo_point,
        zone_hash,
        zone_region,
        speed_kmh,
        fuel_level_pct,
        is_low_fuel,
        is_stopped,
        stop_duration_min,
        -- Ranking para tomar solo el último evento por vehículo
        ROW_NUMBER() OVER (
            PARTITION BY vehicle_id
            ORDER BY event_ts DESC
        ) AS rn
    FROM {{ ref('int_vehicle_h3') }}

    {% if is_incremental() %}
    -- En modo incremental: solo procesar eventos más recientes que lo ya cargado
    WHERE event_ts > (
        SELECT MAX(last_seen_ts) FROM {{ this }}
    )
    {% endif %}
),

final AS (
    SELECT
        vehicle_id,
        event_ts                                AS last_seen_ts,
        latitude,
        longitude,
        geo_point,
        zone_hash,
        zone_region,
        speed_kmh,
        fuel_level_pct,
        is_low_fuel,
        is_stopped,
        COALESCE(stop_duration_min, 0)          AS stop_duration_min,

        -- Lógica de estado del vehículo (prioridad: Alerta > Detenido > En movimiento)
        CASE
            WHEN is_low_fuel                          THEN 'Alerta'
            WHEN is_stopped AND stop_duration_min > 30 THEN 'Alerta'
            WHEN is_stopped                            THEN 'Detenido'
            ELSE 'En movimiento'
        END                                     AS status,

        CURRENT_TIMESTAMP()                     AS updated_at

    FROM latest_events
    WHERE rn = 1   -- Solo el evento más reciente por vehículo
)

SELECT * FROM final
