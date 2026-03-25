"""
infra/setup_gcp.py
==================
Script declarativo para crear todos los recursos GCP de la Fase 1:
  - Dataset BigQuery `logistics_raw` + tabla `telemetry_events`
  - Topic Pub/Sub `fleet-telemetry` + Dead Letter Topic `fleet-telemetry-dlq`

USO:
    python infra/setup_gcp.py

PREREQUISITOS:
    gcloud auth application-default login
    Variables de entorno cargadas desde .env (python-dotenv)
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery, pubsub_v1
from google.api_core.exceptions import AlreadyExists, Conflict
from loguru import logger

# Cargar .env desde la raíz del proyecto
load_dotenv(Path(__file__).parent.parent / ".env")

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
PROJECT_ID  = os.environ["GCP_PROJECT_ID"]
REGION      = os.getenv("GCP_REGION", "us-central1")
BQ_DATASET  = os.getenv("BQ_DATASET", "logistics_raw")
BQ_TABLE    = os.getenv("BQ_TABLE", "telemetry_events")
TOPIC_ID    = os.getenv("PUBSUB_TOPIC_ID", "fleet-telemetry")
DLQ_TOPIC   = os.getenv("PUBSUB_DLQ_TOPIC_ID", "fleet-telemetry-dlq")


# ---------------------------------------------------------------------------
# BigQuery
# ---------------------------------------------------------------------------
TELEMETRY_SCHEMA = [
    bigquery.SchemaField("vehicle_id",       "STRING",    mode="REQUIRED",
                         description="Identificador único del vehículo"),
    bigquery.SchemaField("event_timestamp",  "TIMESTAMP", mode="REQUIRED",
                         description="Timestamp del evento GPS"),
    bigquery.SchemaField("latitude",         "FLOAT64",   mode="NULLABLE",
                         description="Latitud WGS-84"),
    bigquery.SchemaField("longitude",        "FLOAT64",   mode="NULLABLE",
                         description="Longitud WGS-84"),
    bigquery.SchemaField("geo_point",        "GEOGRAPHY", mode="NULLABLE",
                         description="Punto geográfico ST_GEOGPOINT(lon, lat)"),
    bigquery.SchemaField("speed",            "FLOAT64",   mode="NULLABLE",
                         description="Velocidad en km/h"),
    bigquery.SchemaField("fuel_level",       "FLOAT64",   mode="NULLABLE",
                         description="Nivel de combustible (0–100%)"),
    bigquery.SchemaField("_ingested_at",     "TIMESTAMP", mode="NULLABLE",
                         description="Timestamp de ingesta en BigQuery"),
]


def create_bq_dataset(client: bigquery.Client) -> None:
    """Crea el dataset BigQuery si no existe."""
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{BQ_DATASET}")
    dataset_ref.location = REGION
    dataset_ref.description = "Datos crudos de telemetría de la flota logística"
    try:
        client.create_dataset(dataset_ref, timeout=30)
        logger.success(f"Dataset creado: {PROJECT_ID}.{BQ_DATASET}")
    except Conflict:
        logger.info(f"Dataset ya existe: {PROJECT_ID}.{BQ_DATASET} — omitiendo")


def create_bq_table(client: bigquery.Client) -> None:
    """Crea la tabla telemetry_events con partición por día si no existe."""
    table_ref = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    table = bigquery.Table(table_ref, schema=TELEMETRY_SCHEMA)

    # Partición diaria por event_timestamp → reduce costos de query
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="event_timestamp",
        expiration_ms=None,  # sin expiración
    )
    # Clustering por vehicle_id → escaneos más eficientes por vehículo
    table.clustering_fields = ["vehicle_id"]

    try:
        client.create_table(table)
        logger.success(f"Tabla creada: {table_ref}")
        logger.info("  Partición: DAY por event_timestamp")
        logger.info("  Clustering: vehicle_id")
    except Conflict:
        logger.info(f"Tabla ya existe: {table_ref} — omitiendo")


# ---------------------------------------------------------------------------
# Pub/Sub
# ---------------------------------------------------------------------------

def create_pubsub_topic(publisher: pubsub_v1.PublisherClient, topic_id: str) -> str:
    """Crea un topic Pub/Sub y devuelve su path."""
    topic_path = publisher.topic_path(PROJECT_ID, topic_id)
    try:
        publisher.create_topic(request={"name": topic_path})
        logger.success(f"Topic creado: {topic_path}")
    except AlreadyExists:
        logger.info(f"Topic ya existe: {topic_path} — omitiendo")
    return topic_path


def create_pubsub_subscription(
    subscriber: pubsub_v1.SubscriberClient,
    topic_path: str,
    dlq_topic_path: str,
) -> None:
    """
    Crea una suscripción con Dead Letter Policy al topic principal.
    La Cloud Function puede consumir de aquí en caso de prueba local.
    """
    sub_path = subscriber.subscription_path(PROJECT_ID, f"{TOPIC_ID}-sub")
    try:
        subscriber.create_subscription(
            request={
                "name": sub_path,
                "topic": topic_path,
                "dead_letter_policy": {
                    "dead_letter_topic": dlq_topic_path,
                    "max_delivery_attempts": 5,
                },
                "ack_deadline_seconds": 60,
            }
        )
        logger.success(f"Suscripción creada: {sub_path}")
    except AlreadyExists:
        logger.info(f"Suscripción ya existe: {sub_path} — omitiendo")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("=" * 60)
    logger.info("Real-Time Logistics Control Tower — Setup GCP")
    logger.info(f"Proyecto : {PROJECT_ID}")
    logger.info(f"Región   : {REGION}")
    logger.info("=" * 60)

    # BigQuery
    logger.info("\n📦 Configurando BigQuery...")
    bq_client = bigquery.Client(project=PROJECT_ID)
    create_bq_dataset(bq_client)
    create_bq_table(bq_client)

    # Pub/Sub
    logger.info("\n📡 Configurando Pub/Sub...")
    publisher  = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    dlq_path   = create_pubsub_topic(publisher, DLQ_TOPIC)
    topic_path = create_pubsub_topic(publisher, TOPIC_ID)
    create_pubsub_subscription(subscriber, topic_path, dlq_path)

    logger.info("\n✅ Setup completado.")
    logger.info("Próximo paso: autenticar con Application Default Credentials:")
    logger.info("  gcloud auth application-default login")


if __name__ == "__main__":
    try:
        main()
    except KeyError as e:
        logger.error(f"Variable de entorno no definida: {e}")
        logger.error("Asegúrate de tener un archivo .env con GCP_PROJECT_ID")
        sys.exit(1)
