"""
injector/direct_ingest.py
=========================
Script de ingesta directa: NYC Taxi → BigQuery
Útil para backfills, pruebas de datos y cuando la Cloud Function
no está disponible. Bypassa Pub/Sub e inserta directo en BQ.

USO:
    python injector/direct_ingest.py --limit 500
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from loguru import logger

load_dotenv(Path(__file__).parent.parent / ".env")

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BQ_DATASET = os.getenv("BQ_DATASET", "logistics_raw")
BQ_TABLE   = os.getenv("BQ_TABLE", "telemetry_events")
TABLE_REF  = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
NYC_TAXI_URL = os.getenv(
    "NYC_TAXI_URL",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
)

NYC_BBOX = {
    "lat_min": 40.4774, "lat_max": 40.9176,
    "lon_min": -74.2591, "lon_max": -73.7004,
}
NUM_VEHICLES = 200


def load_and_prepare(limit: int) -> list[dict]:
    """Carga el dataset NYC Taxi y lo transforma al esquema de telemetría."""
    import random

    cache_path = Path(__file__).parent / ".cache" / "nyc_taxi_2024_01.parquet"

    if cache_path.exists():
        logger.info(f"Usando caché: {cache_path}")
    else:
        import requests
        logger.info("Descargando dataset NYC Taxi (~40MB)...")
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        r = requests.get(NYC_TAXI_URL, stream=True, timeout=60)
        r.raise_for_status()
        with open(cache_path, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        logger.success("Descarga completa.")

    df = pd.read_parquet(cache_path)
    df = df[["tpep_pickup_datetime", "trip_distance", "VendorID"]].dropna()

    if limit > 0:
        df = df.head(limit)

    rng = random.Random(42)
    vehicle_pool = [f"VH-{str(i).zfill(4)}" for i in range(1, NUM_VEHICLES + 1)]
    fuel_map = {v: round(rng.uniform(20, 100), 1) for v in vehicle_pool}
    ingested_at = datetime.now(timezone.utc).isoformat()

    rows = []
    for i, (_, row) in enumerate(df.iterrows()):
        lat = rng.uniform(NYC_BBOX["lat_min"], NYC_BBOX["lat_max"])
        lon = rng.uniform(NYC_BBOX["lon_min"], NYC_BBOX["lon_max"])
        vid = vehicle_pool[i % NUM_VEHICLES]
        speed = round(float(row.get("trip_distance", 0)) * 1.609 / (5 / 60), 2)

        rows.append({
            "vehicle_id":      vid,
            "event_timestamp": pd.to_datetime(row["tpep_pickup_datetime"]).strftime("%Y-%m-%dT%H:%M:%S"),
            "latitude":        round(lat, 6),
            "longitude":       round(lon, 6),
            "geo_point":       f"POINT({lon} {lat})",
            "speed":           min(speed, 120.0),
            "fuel_level":      fuel_map[vid],
            "_ingested_at":    ingested_at,
        })

    return rows


def insert_to_bigquery(rows: list[dict], batch_size: int = 500) -> None:
    """Inserta filas en BigQuery en batches."""
    client = bigquery.Client(project=PROJECT_ID)
    total   = len(rows)
    success = 0
    errors  = 0

    logger.info(f"Insertando {total:,} filas en {TABLE_REF} (batch={batch_size})...")

    for i in range(0, total, batch_size):
        batch = rows[i:i + batch_size]
        errs  = client.insert_rows_json(TABLE_REF, batch)
        if errs:
            errors += len(errs)
            logger.warning(f"Batch {i//batch_size + 1}: {len(errs)} errores → {errs[0]}")
        else:
            success += len(batch)
            pct = (i + len(batch)) / total * 100
            logger.info(f"  {i + len(batch):,}/{total:,} filas insertadas ({pct:.0f}%)")

    logger.success(f"Completado: {success:,} insertadas, {errors:,} errores.")


def main(limit: int) -> None:
    logger.info("=" * 55)
    logger.info("Direct Ingest: NYC Taxi → BigQuery")
    logger.info(f"Proyecto : {PROJECT_ID}")
    logger.info(f"Tabla    : {TABLE_REF}")
    logger.info(f"Límite   : {limit if limit > 0 else 'sin límite'} filas")
    logger.info("=" * 55)

    rows = load_and_prepare(limit)
    insert_to_bigquery(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingesta directa NYC Taxi → BigQuery")
    parser.add_argument("--limit", type=int, default=500,
                        help="Número máximo de filas a insertar (default: 500)")
    args = parser.parse_args()

    try:
        main(args.limit)
    except KeyError as e:
        logger.error(f"Variable de entorno faltante: {e}")
        sys.exit(1)
