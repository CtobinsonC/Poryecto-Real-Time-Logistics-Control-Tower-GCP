"""
cloud_function/main.py
======================
Cloud Function disparada por Pub/Sub para ingestar eventos de telemetría
de la flota en BigQuery.

Trigger : Pub/Sub (topic: fleet-telemetry)
Runtime : Python 3.12

Variables de entorno requeridas en Cloud Functions:
    GCP_PROJECT_ID
    BQ_DATASET      (default: logistics_raw)
    BQ_TABLE        (default: telemetry_events)
"""

import base64
import json
import os
from datetime import datetime, timezone
from typing import Any

import functions_framework
from google.cloud import bigquery
from loguru import logger
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from google.api_core.exceptions import GoogleAPIError

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BQ_DATASET = os.getenv("BQ_DATASET", "logistics_raw")
BQ_TABLE   = os.getenv("BQ_TABLE", "telemetry_events")
TABLE_REF  = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

# Cliente BigQuery (singleton — reutilizado entre invocaciones en caliente)
_bq_client: bigquery.Client | None = None


def get_bq_client() -> bigquery.Client:
    """Retorna (o crea) el cliente BigQuery como singleton."""
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=PROJECT_ID)
    return _bq_client


# ---------------------------------------------------------------------------
# Validación y transformación
# ---------------------------------------------------------------------------

REQUIRED_FIELDS = {"vehicle_id", "timestamp", "latitude", "longitude"}


def parse_pubsub_message(event: dict) -> dict:
    """
    Decodifica el mensaje Pub/Sub (base64 JSON) y lo transforma al
    esquema de BigQuery.

    Args:
        event: Evento Pub/Sub raw con el campo 'data'.

    Returns:
        Dict listo para insertar en BigQuery.

    Raises:
        ValueError: Si faltan campos obligatorios o las coordenadas son inválidas.
        json.JSONDecodeError: Si el payload no es JSON válido.
    """
    # 1. Decodificar base64
    raw_data = base64.b64decode(event["data"]).decode("utf-8")
    payload: dict = json.loads(raw_data)

    # 2. Validar campos obligatorios
    missing = REQUIRED_FIELDS - payload.keys()
    if missing:
        raise ValueError(f"Campos obligatorios faltantes: {missing}")

    # 3. Extraer y validar coordenadas
    lat = float(payload["latitude"])
    lon = float(payload["longitude"])

    if not (-90 <= lat <= 90):
        raise ValueError(f"Latitud inválida: {lat}. Debe estar entre -90 y 90.")
    if not (-180 <= lon <= 180):
        raise ValueError(f"Longitud inválida: {lon}. Debe estar entre -180 y 180.")

    # 4. Construir fila BigQuery
    row = {
        "vehicle_id":      str(payload["vehicle_id"]),
        "event_timestamp": payload["timestamp"],
        "latitude":        lat,
        "longitude":       lon,
        # ST_GEOGPOINT expresado como WKT para la API de inserción
        "geo_point":       f"POINT({lon} {lat})",
        "speed":           float(payload.get("speed", 0.0)),
        "fuel_level":      float(payload.get("fuel_level", 100.0)),
        "_ingested_at":    datetime.now(timezone.utc).isoformat(),
    }
    return row


# ---------------------------------------------------------------------------
# Inserción en BigQuery (con reintentos)
# ---------------------------------------------------------------------------

@retry(
    retry=retry_if_exception_type(GoogleAPIError),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)
def insert_row_to_bigquery(row: dict) -> None:
    """
    Inserta una fila en BigQuery usando streaming insert.
    Reintenta hasta 3 veces con backoff exponencial ante errores de la API.

    Args:
        row: Diccionario con los datos del evento.

    Raises:
        RuntimeError: Si BigQuery devuelve errores de inserción.
        GoogleAPIError: Si la API falla y se agotan los reintentos.
    """
    client = get_bq_client()
    errors = client.insert_rows_json(TABLE_REF, [row])

    if errors:
        error_msg = json.dumps(errors)
        logger.error(f"Errores de inserción BigQuery: {error_msg}")
        raise RuntimeError(f"BigQuery insert_rows_json falló: {error_msg}")

    logger.info(f"Evento insertado | vehicle_id={row['vehicle_id']} | ts={row['event_timestamp']}")


# ---------------------------------------------------------------------------
# Entry point — Cloud Function
# ---------------------------------------------------------------------------

@functions_framework.cloud_event
def ingest_telemetry(cloud_event: Any) -> None:
    """
    Entry point de la Cloud Function (Gen 2, CloudEvents).

    Recibe un evento de Pub/Sub, parsea el payload y lo inserta en BigQuery.
    Los mensajes que fallen repetidamente serán redirigidos al Dead Letter Topic.

    Args:
        cloud_event: CloudEvent del trigger de Pub/Sub.
    """
    try:
        # Extraer el evento Pub/Sub del CloudEvent
        pubsub_event = {"data": cloud_event.data["message"]["data"]}

        # Parsear y validar el mensaje
        row = parse_pubsub_message(pubsub_event)

        # Insertar en BigQuery
        insert_row_to_bigquery(row)

    except (ValueError, json.JSONDecodeError) as e:
        # Errores de validación: no reintentar (mensaje malformado → DLQ)
        logger.error(f"Error de validación, mensaje ignorado: {e}")
        # No re-lanzar — evita que Pub/Sub reintente mensajes inválidos infinitamente

    except RuntimeError as e:
        # Errores de BQ después de reintentos → dejar que Pub/Sub reintente
        logger.error(f"Error de inserción BQ (Pub/Sub reintentará): {e}")
        raise

    except Exception as e:
        logger.error(f"Error inesperado: {e}")
        raise
