from google.cloud import bigquery

client = bigquery.Client(project='gcp-real-time-logistics-cont')
query = """
DELETE FROM `gcp-real-time-logistics-cont.logistics_marts.fct_fleet_status`
WHERE vehicle_id > 'VH-0200'
"""
job = client.query(query)
job.result()
print("Operación completada: Se han eliminado los vehículos > 200 del Data Mart.")
