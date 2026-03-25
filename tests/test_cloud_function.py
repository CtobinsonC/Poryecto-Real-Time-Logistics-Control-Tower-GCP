"""
tests/test_cloud_function.py
=============================
Tests unitarios para la Cloud Function de ingesta de telemetría.

Ejecutar con:
    pytest tests/ -v --cov=cloud_function --cov-report=term-missing
"""

import base64
import json
import os
from unittest.mock import MagicMock, patch

import pytest

# Configurar variables de entorno ANTES de importar el módulo
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("BQ_DATASET", "logistics_raw")
os.environ.setdefault("BQ_TABLE", "telemetry_events")

# Importar funciones del módulo a testear
import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent / "cloud_function"))

from main import parse_pubsub_message, insert_row_to_bigquery


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_pubsub_event(payload: dict) -> dict:
    """Construye un evento Pub/Sub simulado con payload JSON en base64."""
    raw = json.dumps(payload).encode("utf-8")
    return {"data": base64.b64encode(raw).decode("utf-8")}


VALID_PAYLOAD = {
    "vehicle_id":  "VH-0001",
    "timestamp":   "2024-01-15T10:30:00",
    "latitude":    40.7128,
    "longitude":  -74.0060,
    "speed":       65.5,
    "fuel_level":  78.2,
}


# ---------------------------------------------------------------------------
# Test 1: Mensaje válido → parseo correcto
# ---------------------------------------------------------------------------

def test_valid_message_parses_correctly():
    """Un mensaje bien formado debe ser parseado al esquema de BigQuery."""
    event = make_pubsub_event(VALID_PAYLOAD)
    row = parse_pubsub_message(event)

    assert row["vehicle_id"] == "VH-0001"
    assert row["latitude"]   == 40.7128
    assert row["longitude"]  == -74.0060
    assert row["speed"]      == 65.5
    assert row["fuel_level"] == 78.2
    assert "geo_point" in row
    assert "POINT" in row["geo_point"]
    assert "_ingested_at" in row


# ---------------------------------------------------------------------------
# Test 2: Mensaje válido → insert en BigQuery exitoso
# ---------------------------------------------------------------------------

@patch("main.get_bq_client")
def test_valid_message_inserts_to_bq(mock_get_client):
    """Un mensaje válido debe provocar un insert exitoso en BigQuery."""
    mock_client = MagicMock()
    mock_client.insert_rows_json.return_value = []  # Sin errores
    mock_get_client.return_value = mock_client

    event = make_pubsub_event(VALID_PAYLOAD)
    row = parse_pubsub_message(event)
    insert_row_to_bigquery(row)

    mock_client.insert_rows_json.assert_called_once()
    call_args = mock_client.insert_rows_json.call_args
    inserted_rows = call_args[0][1]
    assert inserted_rows[0]["vehicle_id"] == "VH-0001"


# ---------------------------------------------------------------------------
# Test 3: Campos obligatorios faltantes → ValueError
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("missing_field", [
    "vehicle_id", "timestamp", "latitude", "longitude"
])
def test_missing_required_field_raises_value_error(missing_field):
    """Cualquier campo obligatorio faltante debe lanzar ValueError."""
    payload = {**VALID_PAYLOAD}
    del payload[missing_field]

    event = make_pubsub_event(payload)
    with pytest.raises(ValueError, match="Campos obligatorios faltantes"):
        parse_pubsub_message(event)


# ---------------------------------------------------------------------------
# Test 4: Coordenadas fuera de rango → ValueError
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("lat, lon, description", [
    (91.0,   -74.0, "latitud > 90"),
    (-91.0,  -74.0, "latitud < -90"),
    (40.7,   181.0, "longitud > 180"),
    (40.7,  -181.0, "longitud < -180"),
])
def test_invalid_coordinates_raise_value_error(lat, lon, description):
    """Coordenadas fuera de rango deben lanzar ValueError."""
    payload = {**VALID_PAYLOAD, "latitude": lat, "longitude": lon}
    event = make_pubsub_event(payload)

    with pytest.raises(ValueError):
        parse_pubsub_message(event)


# ---------------------------------------------------------------------------
# Test 5: BigQuery devuelve errores → RuntimeError + reintento
# ---------------------------------------------------------------------------

@patch("main.get_bq_client")
def test_bq_error_raises_runtime_error(mock_get_client):
    """Si BigQuery devuelve errores, debe lanzarse RuntimeError."""
    mock_client = MagicMock()
    mock_client.insert_rows_json.return_value = [
        {"index": 0, "errors": [{"reason": "invalid", "message": "test error"}]}
    ]
    mock_get_client.return_value = mock_client

    event = make_pubsub_event(VALID_PAYLOAD)
    row = parse_pubsub_message(event)

    with pytest.raises(RuntimeError, match="BigQuery insert_rows_json falló"):
        # tenacity reintentará 3 veces → todas fallarán → RuntimeError final
        insert_row_to_bigquery.retry.statistics  # acceder al objeto retry
        insert_row_to_bigquery(row)


# ---------------------------------------------------------------------------
# Test 6: Payload JSON inválido → JSONDecodeError
# ---------------------------------------------------------------------------

def test_invalid_json_raises_decode_error():
    """Un payload que no es JSON válido debe lanzar json.JSONDecodeError."""
    import json
    raw = b"esto no es json {"
    event = {"data": base64.b64encode(raw).decode("utf-8")}

    with pytest.raises(json.JSONDecodeError):
        parse_pubsub_message(event)


# ---------------------------------------------------------------------------
# Test 7: Campos opcionales con valores por defecto
# ---------------------------------------------------------------------------

def test_optional_fields_use_defaults():
    """speed y fuel_level son opcionales; deben tener valores por defecto."""
    payload = {
        "vehicle_id": "VH-9999",
        "timestamp":  "2024-01-15T10:30:00",
        "latitude":   40.7128,
        "longitude": -74.0060,
        # Sin speed ni fuel_level
    }
    event = make_pubsub_event(payload)
    row = parse_pubsub_message(event)

    assert row["speed"]      == 0.0
    assert row["fuel_level"] == 100.0
