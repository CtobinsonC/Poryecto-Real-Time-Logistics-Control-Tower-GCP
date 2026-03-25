import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from cachetools import TTLCache

# Configuración de Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuración GCP
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET", "logistics_marts")
BQ_TABLE = os.getenv("BQ_TABLE", "fct_fleet_status")
TABLE_REF = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

# Caching en memoria para no saturar BigQuery
# Guarda 1 elemento (todo el resultado) por N segundos
locations_cache = TTLCache(maxsize=1, ttl=10)
stats_cache = TTLCache(maxsize=1, ttl=30)

bq_client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global bq_client
    # Inicializar cliente BigQuery al arrancar la app
    if PROJECT_ID:
        bq_client = bigquery.Client(project=PROJECT_ID)
    else:
        bq_client = bigquery.Client()
    yield
    # Limpieza al apagar
    if bq_client:
        bq_client.close()

app = FastAPI(
    title="Real-Time Logistics Control Tower API",
    description="Backend API para servir datos de la flota en tiempo real desde BigQuery",
    version="1.0.0",
    lifespan=lifespan
)

# Permitir CORS para el frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"status": "ok", "service": "Logistics API"}

@app.get("/fleet/locations")
def get_fleet_locations():
    """
    Devuelve la última ubicación y estado de todos los vehículos.
    Cacheado por 10 segundos.
    """
    if "data" in locations_cache:
        logger.info("Sirviendo /fleet/locations desde caché (10s)")
        return locations_cache["data"]

    query = f"""
        SELECT 
            vehicle_id, 
            latitude, 
            longitude, 
            speed_kmh, 
            fuel_level_pct, 
            status, 
            zone_hash,
            last_seen_ts 
        FROM `{TABLE_REF}`
    """
    logger.info("Consultando BigQuery para /fleet/locations")
    query_job = bq_client.query(query)
    
    # Extraer resultados
    results = []
    for row in query_job:
        results.append({
            "vehicle_id": row.vehicle_id,
            "latitude": row.latitude,
            "longitude": row.longitude,
            "speed_kmh": row.speed_kmh,
            "fuel_level_pct": row.fuel_level_pct,
            "status": row.status,
            "zone_hash": row.zone_hash,
            "last_seen_ts": row.last_seen_ts.isoformat() if row.last_seen_ts else None
        })

    locations_cache["data"] = results
    return results

@app.get("/fleet/stats")
def get_fleet_stats():
    """
    Devuelve métricas agregadas agrupadas por estado.
    Cacheado por 30 segundos.
    """
    if "data" in stats_cache:
        logger.info("Sirviendo /fleet/stats desde caché (30s)")
        return stats_cache["data"]

    query = f"""
        SELECT 
            status, 
            COUNT(*) as v_count, 
            AVG(fuel_level_pct) as avg_fuel 
        FROM `{TABLE_REF}`
        GROUP BY status
    """
    logger.info("Consultando BigQuery para /fleet/stats")
    query_job = bq_client.query(query)

    details = []
    total_vehicles = 0

    for row in query_job:
        count = row.v_count
        total_vehicles += count
        details.append({
            "status": row.status,
            "count": count,
            "avg_fuel_pct": round(row.avg_fuel, 1) if row.avg_fuel is not None else 0.0
        })

    response = {
        "total_vehicles": total_vehicles,
        "details": details
    }

    stats_cache["data"] = response
    return response

if __name__ == "__main__":
    import uvicorn
    # Puerto por defecto 8080 (requerido por Cloud Run)
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
