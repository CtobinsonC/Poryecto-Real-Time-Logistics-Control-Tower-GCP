"""
injector/nyc_taxi_injector.py
==============================
Script local que simula telemetría de flota publicando mensajes en Pub/Sub,
usando el dataset público de NYC TLC Yellow Taxi Trips como fuente de datos reales.

USO:
    python injector/nyc_taxi_injector.py [--rate N] [--limit N]

ARGUMENTOS:
    --rate  N    Mensajes por segundo a publicar (default: 10)
    --limit N    Máximo de mensajes (default: 0 = ilimitado, recicla el dataset)

PREREQUISITOS:
    gcloud auth application-default login
    Variables de entorno en .env (GCP_PROJECT_ID, PUBSUB_TOPIC_ID)

MAPEO NYC TAXI → Telemetría de flota:
    VendorID            → vehicle_id (prefijado con "VH-")
    tpep_pickup_datetime → timestamp
    pickup_latitude/longitude → coordenadas (aprox. desde trip_distance)
    trip_distance       → speed estimada (km/h)
    — fuel_level es simulado aleatoriamente (20–100%)
"""

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from google.cloud import pubsub_v1
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

# Cargar .env desde la raíz del proyecto
load_dotenv(Path(__file__).parent.parent / ".env")

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
TOPIC_ID   = os.getenv("PUBSUB_TOPIC_ID", "fleet-telemetry")
NYC_TAXI_URL = os.getenv(
    "NYC_TAXI_URL",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
)

# NYC Taxi: bounding box aproximado de NYC para validar coordenadas
NYC_BBOX = {
    "lat_min": 40.4774, "lat_max": 40.9176,
    "lon_min": -74.2591, "lon_max": -73.7004,
}

# Número de vehículos únicos a simular (VendorID tiene pocos valores únicos)
NUM_VEHICLES = 200


# ---------------------------------------------------------------------------
# Descarga del dataset
# ---------------------------------------------------------------------------

def download_nyc_taxi(url: str, cache_path: Path) -> pd.DataFrame:
    """
    Descarga el dataset NYC Taxi en Parquet y lo cachea localmente.
    Si el archivo ya existe, usa el caché.
    """
    if cache_path.exists():
        logger.info(f"Usando caché local: {cache_path}")
    else:
        logger.info(f"Descargando dataset NYC Taxi desde: {url}")
        logger.info("Esto puede tardar unos segundos... (~40MB)")
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.success(f"Dataset descargado: {cache_path}")

    df = pd.read_parquet(cache_path)
    logger.info(f"Dataset cargado: {len(df):,} filas")
    return df


def clean_and_map(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia el dataset NYC Taxi y lo mapea al esquema de telemetría.
    Genera vehicle_ids únicos y simula velocidad + fuel_level.
    """
    # Columnas relevantes
    cols_needed = ["tpep_pickup_datetime", "pickup_longitude", "pickup_latitude",
                   "trip_distance", "VendorID"]

    # El dataset 2024 no tiene lat/lon como columnas separadas
    # → Usamos coordenadas de zonas conocidas de NYC con jitter aleatorio
    available = [c for c in cols_needed if c in df.columns]
    logger.info(f"Columnas disponibles en dataset: {list(df.columns[:10])}...")

    # Si no hay lat/lon explícitas (dataset post-2016), generamos coordenadas
    # dentro del bounding box de NYC con distribución realista
    if "pickup_latitude" not in df.columns:
        logger.info("Dataset sin lat/lon explícitas → generando coordenadas NYC sintéticas")
        df = df[["tpep_pickup_datetime", "trip_distance",
                 "VendorID"]].dropna().head(500_000)

        rng = random.Random(42)
        df["latitude"]  = [
            rng.uniform(NYC_BBOX["lat_min"], NYC_BBOX["lat_max"])
            for _ in range(len(df))
        ]
        df["longitude"] = [
            rng.uniform(NYC_BBOX["lon_min"], NYC_BBOX["lon_max"])
            for _ in range(len(df))
        ]
    else:
        df = df[available].dropna()
        df = df.rename(columns={
            "pickup_latitude":  "latitude",
            "pickup_longitude": "longitude",
        })
        # Filtrar coordenadas dentro de NYC
        df = df[
            df["latitude"].between(NYC_BBOX["lat_min"],  NYC_BBOX["lat_max"]) &
            df["longitude"].between(NYC_BBOX["lon_min"], NYC_BBOX["lon_max"])
        ]

    # Generar vehicle_ids (NUM_VEHICLES distintos, asignados round-robin)
    vehicle_pool = [f"VH-{str(i).zfill(4)}" for i in range(1, NUM_VEHICLES + 1)]
    df["vehicle_id"] = [vehicle_pool[i % NUM_VEHICLES] for i in range(len(df))]

    # Estimar velocidad: trip_distance (millas) → velocidad en km/h (promedio)
    # Asumimos viaje de ~5 min → speed = distance * 1.609 / (5/60) * factor
    if "trip_distance" in df.columns:
        df["speed"] = (df["trip_distance"] * 1.609 / (5 / 60)).clip(0, 120).round(2)
    else:
        df["speed"] = [round(random.uniform(0, 100), 2) for _ in range(len(df))]

    # Fuel level aleatorio (20%–100%) pero consistente por vehicle_id
    rng = random.Random(99)
    fuel_map = {vid: round(rng.uniform(20, 100), 1) for vid in vehicle_pool}
    df["fuel_level"] = df["vehicle_id"].map(fuel_map)

    # Timestamp del evento
    df["timestamp"] = pd.to_datetime(df["tpep_pickup_datetime"]).dt.strftime(
        "%Y-%m-%dT%H:%M:%S"
    )

    return df[["vehicle_id", "timestamp", "latitude", "longitude", "speed", "fuel_level"]]


# ---------------------------------------------------------------------------
# Publicación en Pub/Sub
# ---------------------------------------------------------------------------

def get_publisher() -> tuple[pubsub_v1.PublisherClient, str]:
    """Inicializa el cliente Pub/Sub y devuelve (cliente, topic_path)."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    return publisher, topic_path


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
    reraise=True,
)
def publish_message(publisher: pubsub_v1.PublisherClient,
                    topic_path: str,
                    row: dict) -> None:
    """Publica un mensaje JSON en Pub/Sub con reintentos."""
    data = json.dumps(row, default=str).encode("utf-8")
    future = publisher.publish(topic_path, data=data)
    future.result(timeout=10)  # bloquear hasta confirmación


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(rate: int, limit: int) -> None:
    """
    Loop principal del inyector.

    Args:
        rate:  Mensajes por segundo a publicar.
        limit: Máximo de mensajes (0 = sin límite, recicla el dataset).
    """
    cache_path = Path(__file__).parent / ".cache" / "nyc_taxi_2024_01.parquet"

    logger.info("Iniciando inyector de telemetría de flota")
    logger.info(f"   Proyecto  : {PROJECT_ID}")
    logger.info(f"   Topic     : {TOPIC_ID}")
    logger.info(f"   Rate      : {rate} msg/s")
    logger.info(f"   Límite    : {limit if limit > 0 else 'sin límite'} mensajes")

    # Descargar y preparar datos
    raw_df = download_nyc_taxi(NYC_TAXI_URL, cache_path)
    df     = clean_and_map(raw_df)

    logger.success(f"Dataset preparado: {len(df):,} filas disponibles")

    # Inicializar Pub/Sub
    publisher, topic_path = get_publisher()

    interval    = 1.0 / rate
    total_sent  = 0
    total_errors = 0

    logger.info("\n▶ Publicando mensajes... (Ctrl+C para detener)\n")

    try:
        while True:
            # Reciclar el dataset si es necesario
            for _, row in df.iterrows():
                if limit > 0 and total_sent >= limit:
                    logger.success(f"Límite alcanzado: {total_sent} mensajes publicados.")
                    return

                payload = row.to_dict()
                try:
                    publish_message(publisher, topic_path, payload)
                    total_sent += 1
                    if total_sent % 100 == 0:
                        logger.info(
                            f" {total_sent:,} mensajes publicados | "
                            f" {total_errors} errores | "
                            f"Último vehicle: {payload['vehicle_id']}"
                        )
                except Exception as e:
                    total_errors += 1
                    logger.warning(f"Error publicando mensaje: {e}")

                time.sleep(interval)

            if limit == 0:
                logger.info("Dataset completo → reciclando desde el inicio...")

    except KeyboardInterrupt:
        logger.info(f"\nInyector detenido. Total publicados: {total_sent:,}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inyector de telemetría de flota vía NYC Taxi → Pub/Sub"
    )
    parser.add_argument(
        "--rate", type=int,
        default=int(os.getenv("INJECTOR_RATE", "10")),
        help="Mensajes por segundo (default: 10)",
    )
    parser.add_argument(
        "--limit", type=int,
        default=int(os.getenv("INJECTOR_LIMIT", "0")),
        help="Máximo de mensajes a publicar, 0 = sin límite (default: 0)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        run(rate=args.rate, limit=args.limit)
    except KeyError as e:
        logger.error(f"Variable de entorno no definida: {e}")
        logger.error("Asegúrate de tener un archivo .env con GCP_PROJECT_ID")
        sys.exit(1)
